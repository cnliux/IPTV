import asyncio
import aiohttp
import time
import re
import logging
import os
import gc
from typing import List, Set, Tuple, Optional, Dict, Callable
from collections import defaultdict
from urllib.parse import urlparse
from configparser import ConfigParser
from .models import Channel

logger = logging.getLogger(__name__)

class SpeedTester:
    """高性能流媒体测速引擎（完整修复版）"""

    def __init__(self, 
                 timeout: float = 5.0, 
                 concurrency: int = 10, 
                 max_attempts: int = 3,
                 min_download_speed: float = 100.0, 
                 enable_logging: bool = True,
                 config: Optional[ConfigParser] = None):
        """
        初始化测速器（修复Windows文件描述符限制）
        """
        # Windows系统限制并发数
        if os.name == 'nt':
            max_concurrency = min(concurrency, 30)  # Windows限制为30
            if concurrency > max_concurrency:
                logger.warning(f"Windows系统并发数限制为{max_concurrency}（原{concurrency}）")
            self.concurrency = max_concurrency
        else:
            self.concurrency = max(1, min(concurrency, 100))
        
        # 基础配置
        self.timeout = timeout
        self.max_attempts = max(1, max_attempts)
        self.min_download_speed = max(0.1, min_download_speed)
        self._enable_logging = enable_logging
        self.config = config or ConfigParser()

        # 下载限制配置
        self.max_download_size = self.config.getint(
            'TESTER', 
            'max_download_size', 
            fallback=100 * 1024
        )
        
        # 初始化日志系统
        self._init_logger()

        # 协议检测
        self.rtp_udp_pattern = re.compile(r'/(rtp|udp)/', re.IGNORECASE)
        
        # 协议特定配置
        self.udp_timeout = self.config.getfloat('TESTER', 'udp_timeout', fallback=max(0.5, timeout * 0.3))
        self.http_timeout = self.config.getfloat('TESTER', 'http_timeout', fallback=timeout)
        self.min_udp_download_speed = self.config.getfloat('TESTER', 'min_udp_download_speed', fallback=30)
        self.max_udp_latency = self.config.getint('TESTER', 'max_udp_latency', fallback=300)
        self.max_http_latency = self.config.getint('TESTER', 'max_http_latency', fallback=1000)
        self.max_channels_per_ip = self.config.getint('TESTER', 'max_channels_per_ip', fallback=100)
        
        # IP防护机制
        self.failed_ips: Dict[str, int] = defaultdict(int)
        self.max_failures_per_ip = self.config.getint('PROTECTION', 'max_failures_per_ip', fallback=5)
        self.blocked_ips: Set[str] = set()
        self.ip_cooldown: Dict[str, float] = {}
        self.min_ip_interval = self.config.getfloat('PROTECTION', 'min_ip_interval', fallback=0.5)
        
        # 并发控制（修复关键）
        self._active_tasks = 0
        self._max_active_tasks = self.concurrency
        self._task_condition = asyncio.Condition()
        
        # 统计
        self.success_count = 0
        self.total_count = 0
        self.start_time = 0.0

    def _init_logger(self):
        """初始化日志记录器"""
        self.logger = logging.getLogger('core.tester')
        self.logger.disabled = not self._enable_logging
        
        def make_log_method(level):
            def log_method(msg, *args, **kwargs):
                if self._enable_logging:
                    getattr(self.logger, level)(msg, *args, **kwargs)
            return log_method
        
        self.log = type('LogMethod', (), {
            'debug': make_log_method('debug'),
            'info': make_log_method('info'),
            'warning': make_log_method('warning'),
            'error': make_log_method('error'),
            'exception': make_log_method('exception')
        })()

    async def test_channels(self, 
                          channels: List[Channel], 
                          progress_cb: Optional[Callable] = None,
                          failed_urls: Optional[Set[str]] = None, 
                          white_list: Optional[Set[str]] = None) -> None:
        """
        批量测试频道（安全版本，修复文件描述符限制）
        """
        failed_urls = failed_urls or set()
        white_list = white_list or set()
        progress_cb = progress_cb or (lambda _: None)
        
        self.total_count = len(channels)
        self.success_count = 0
        self.start_time = time.time()
        
        self.log.info(
            "▶️ 开始测速 | 总数: %d | 并发: %d | 单IP最大频道: %d",
            self.total_count, self.concurrency, self.max_channels_per_ip
        )

        # 创建自定义connector（关键修复）
        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            force_close=True,
            enable_cleanup_closed=True,
            ssl=False
        )

        try:
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as session:
                # 使用带限制的批处理
                await self._process_batch_with_limits(
                    session, channels, progress_cb, failed_urls, white_list
                )
                
        except Exception as e:
            self.log.error("测试过程中发生错误: %s", str(e))
            if "_abort" not in str(e):
                raise
        finally:
            await connector.close()
            elapsed = time.time() - self.start_time
            success_rate = (self.success_count / self.total_count) * 100 if self.total_count > 0 else 0
            self.log.info(
                "✅ 测速完成 | 成功: %d(%.1f%%) | 失败: %d | 屏蔽IP: %d | 用时: %.1fs",
                self.success_count, success_rate,
                self.total_count - self.success_count,
                len(self.blocked_ips),
                elapsed
            )

    async def _process_batch_with_limits(self, session, channels, progress_cb, failed_urls, white_list):
        """带连接数限制的批处理"""
        tasks = []
        for channel in channels:
            # 等待可用槽位
            async with self._task_condition:
                await self._task_condition.wait_for(
                    lambda: self._active_tasks < self._max_active_tasks
                )
                self._active_tasks += 1
            
            # 创建任务
            task = asyncio.create_task(
                self._test_single_channel_limited(session, channel, progress_cb, failed_urls, white_list)
            )
            tasks.append(task)
        
        # 安全执行，避免异常传播
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理异常结果
        for result in results:
            if isinstance(result, Exception):
                self.log.error("任务执行异常: %s", str(result))

    async def _test_single_channel_limited(self, session, channel, progress_cb, failed_urls, white_list):
        """带资源限制的单频道测试"""
        try:
            await self._test_single_channel(session, channel, progress_cb, failed_urls, white_list)
        except Exception as e:
            self.log.error("频道测试异常 %s: %s", channel.name, str(e))
            failed_urls.add(channel.url)
            channel.status = 'offline'
        finally:
            # 释放槽位
            async with self._task_condition:
                self._active_tasks -= 1
                self._task_condition.notify_all()
            progress_cb(1)

    async def _test_single_channel(self,
                                 session: aiohttp.ClientSession,
                                 channel: Channel,
                                 progress_cb: Callable,
                                 failed_urls: Set[str],
                                 white_list: Set[str]) -> None:
        """测试单个频道"""
        if self._is_in_white_list(channel, white_list):
            channel.status = 'online'
            self.log.debug("🟢 白名单跳过 %s", channel.name)
            return

        if channel.url in self.blocked_ips:
            channel.status = 'offline'
            failed_urls.add(channel.url)
            return

        try:
            self.log.debug("🔍 开始测试 %s", channel.name)

            success, speed, latency = await self._unified_test(session, channel)
            
            if success:
                self._handle_success(channel, speed, latency)
            else:
                self._handle_failure(channel, failed_urls, speed, latency)
                
        except asyncio.TimeoutError:
            self._handle_timeout(channel, failed_urls)
        except aiohttp.ClientError as e:
            self._handle_client_error(channel, failed_urls, e)
        except Exception as e:
            self._handle_error(channel, failed_urls, e)

    async def _unified_test(self,
                          session: aiohttp.ClientSession,
                          channel: Channel) -> Tuple[bool, float, float]:
        """统一测试方法（支持UDP/HTTP协议）"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            is_udp = self._is_udp_url(channel.url)
            timeout_val = self.udp_timeout if is_udp else self.http_timeout
            
            # 协议阈值
            min_speed = self.min_udp_download_speed if is_udp else self.min_download_speed
            max_latency = self.max_udp_latency if is_udp else self.max_http_latency

            # 阶段1：快速HEAD请求测延迟
            latency_start = time.perf_counter()
            async with session.head(channel.url, headers=headers, timeout=timeout_val) as resp:
                latency = (time.perf_counter() - latency_start) * 1000
                if latency > max_latency or resp.status != 200:
                    return False, 0.0, latency

            # 阶段2：GET请求测速度（复用连接）
            start = time.perf_counter()
            content_size = 0
            
            # 使用iter_chunked分块读取，避免一次性加载大文件
            async with session.get(channel.url, headers=headers, timeout=timeout_val) as resp:
                async for chunk in resp.content.iter_chunked(1024 * 4):  # 4KB chunks
                    content_size += len(chunk)
                    # 达到最大下载量时提前结束
                    if content_size >= self.max_download_size:
                        break
                
                duration = time.perf_counter() - start
                speed = content_size / duration / 1024 if duration > 0 else 0
                return speed >= min_speed, speed, latency

        except asyncio.TimeoutError:
            return False, 0.0, 0.0
        except aiohttp.ClientError:
            return False, 0.0, 0.0
        except Exception:
            return False, 0.0, 0.0

    def _handle_success(self,
                      channel: Channel,
                      speed: float,
                      latency: float) -> None:
        """处理成功结果"""
        self.success_count += 1
        channel.status = 'online'
        channel.response_time = latency
        channel.download_speed = speed
        
        protocol = "UDP" if self._is_udp_url(channel.url) else "HTTP"
        self.log.info(
            "✅ 成功 | %-5s | %-30s | %6.1fKB/s | %4.0fms | %s",
            protocol, channel.name[:30], speed, latency,
            self._simplify_url(channel.url)
        )

    def _handle_failure(self,
                       channel: Channel,
                       failed_urls: Set[str],
                       speed: float,
                       latency: float) -> None:
        """处理失败结果"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        is_udp = self._is_udp_url(channel.url)
        reason = (
            "速度不足" if speed > 0 and speed < (
                self.min_udp_download_speed if is_udp else self.min_download_speed
            ) else
            "延迟过高" if latency > (
                self.max_udp_latency if is_udp else self.max_http_latency
            ) else
            "连接失败"
        )
        
        self.log.warning(
            "❌ 失败 | %-5s | %-30s | %6.1fKB/s | %4.0fms | %-8s | %s",
            "UDP" if is_udp else "HTTP",
            channel.name[:30], speed, latency, reason,
            self._simplify_url(channel.url)
        )

    def _handle_timeout(self,
                       channel: Channel,
                       failed_urls: Set[str]) -> None:
        """处理超时"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        self.log.warning(
            "⏰ 超时 | %-30s | %s",
            channel.name[:30],
            self._simplify_url(channel.url)
        )

    def _handle_client_error(self,
                           channel: Channel,
                           failed_urls: Set[str],
                           error: Exception) -> None:
        """处理客户端错误"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        self.log.error(
            "🌐 客户端错误 | %-30s | %-20s | %s",
            channel.name[:30], str(error)[:20],
            self._simplify_url(channel.url)
        )

    def _handle_error(self,
                     channel: Channel,
                     failed_urls: Set[str],
                     error: Exception) -> None:
        """处理异常"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        self.log.error(
            "‼️ 异常 | %-30s | %-20s | %s",
            channel.name[:30], str(error)[:20],
            self._simplify_url(channel.url)
        )

    def _is_udp_url(self, url: str) -> bool:
        """判断是否为UDP协议URL"""
        url_lower = url.lower()
        return (url_lower.startswith(('udp://', 'rtp://')) or 
                bool(self.rtp_udp_pattern.search(url_lower)))

    def _extract_ip_from_url(self, url: str) -> str:
        """从URL提取IP地址"""
        try:
            parsed = urlparse(url)
            netloc = parsed.netloc.split('@')[-1]
            if '[' in netloc and ']' in netloc:  # IPv6
                return netloc.split(']')[0] + ']'
            return netloc.split(':')[0]  # IPv4
        except:
            return "unknown"

    def _simplify_url(self, url: str) -> str:
        """简化URL显示"""
        return url[:100] + '...' if len(url) > 100 else url

    def _is_in_white_list(self,
                        channel: Channel,
                        white_list: Set[str]) -> bool:
        """检查是否在白名单中"""
        if not white_list:
            return False
        return channel.name.lower() in white_list

    def _group_channels_by_ip(self, 
                            channels: List[Channel],
                            white_list: Set[str]) -> Dict[str, List[Channel]]:
        """
        改进版IP分组逻辑
        返回: { "ip_0": [ch1,ch2...], "ip_1": [...] }
        """
        groups = defaultdict(list)
        ip_counter = defaultdict(int)
        
        # 白名单独立分组
        whitelist_group = [ch for ch in channels if self._is_in_white_list(ch, white_list)]
        if whitelist_group:
            groups["whitelist"] = whitelist_group
        
        # 常规分组
        for ch in channels:
            if self._is_in_white_list(ch, white_list):
                continue
                
            ip = self._extract_ip_from_url(ch.url)
            group_idx = ip_counter[ip] // self.max_channels_per_ip
            group_key = f"{ip}_{group_idx}"
            
            groups[group_key].append(ch)
            ip_counter[ip] += 1
            
            self.log.debug("IP %s 频道数超过 %d，创建分组 %s", 
                         ip, self.max_channels_per_ip, group_key)
        
        return groups

    def clear_resources(self):
        """清理资源"""
        self.failed_ips.clear()
        self.blocked_ips.clear()
        self.ip_cooldown.clear()
        self._active_tasks = 0
        gc.collect()