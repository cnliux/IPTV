import asyncio
import aiohttp
import time
import re
import logging
from typing import List, Set, Tuple, Optional, Dict, Callable
from collections import defaultdict
from urllib.parse import urlparse
from configparser import ConfigParser
from .models import Channel

logger = logging.getLogger('core.tester')

class SpeedTester:
    """支持协议分离测试的测速引擎（完整中文日志版）"""

    def __init__(self, 
                 timeout: float, 
                 concurrency: int, 
                 max_attempts: int,
                 min_download_speed: float, 
                 enable_logging: bool = True,
                 config: Optional[ConfigParser] = None):
        """
        初始化测速器
        
        参数:
            timeout: 基础超时时间(秒)
            concurrency: 最大并发数
            max_attempts: 最大重试次数
            min_download_speed: HTTP最低速度要求(KB/s)
            enable_logging: 是否启用日志
            config: 配置对象
        """
        # 基础配置
        self.timeout = timeout
        self.concurrency = concurrency
        self.max_attempts = max_attempts
        self.min_download_speed = min_download_speed
        self.enable_logging = enable_logging
        self.config = config or ConfigParser()

        # 协议检测
        self.rtp_udp_pattern = re.compile(r'/(rtp|udp)/', re.IGNORECASE)
        
        # 协议特定配置
        self.udp_timeout = self.config.getfloat('TESTER', 'udp_timeout', fallback=max(0.5, timeout * 0.3))
        self.http_timeout = self.config.getfloat('TESTER', 'http_timeout', fallback=timeout)
        self.min_udp_download_speed = self.config.getfloat('TESTER', 'min_udp_download_speed', fallback=30)
        self.max_udp_latency = self.config.getint('TESTER', 'max_udp_latency', fallback=300)
        self.max_http_latency = self.config.getint('TESTER', 'max_http_latency', fallback=1000)
        
        # 测试模式
        self.udp_test_method = self._get_udp_test_method()
        self.http_test_method = self._get_http_test_method()
        
        # IP防护
        self.ip_cooldown: Dict[str, float] = {}
        self.min_ip_interval = self.config.getfloat('PROTECTION', 'min_ip_interval', fallback=0.5)
        self.failed_ips: Dict[str, int] = defaultdict(int)
        self.max_failures_per_ip = self.config.getint('PROTECTION', 'max_failures_per_ip', fallback=5)
        
        # 并发控制
        self.semaphore = asyncio.Semaphore(concurrency)
        
        # 统计
        self.success_count = 0
        self.total_count = 0
        self.blocked_ips: Set[str] = set()

    def _get_udp_test_method(self) -> str:
        """获取UDP测试模式"""
        method = self.config.get('TESTER', 'udp_test_method', fallback='latency').strip().lower()
        return method if method in ('latency', 'speed') else 'latency'

    def _get_http_test_method(self) -> str:
        """获取HTTP测试模式"""
        method = self.config.get('TESTER', 'http_test_method', fallback='both').strip().lower()
        return method if method in ('latency', 'speed', 'both') else 'both'

    async def test_channels(self, 
                          channels: List[Channel], 
                          progress_cb: Callable,
                          failed_urls: Set[str], 
                          white_list: Set[str]) -> None:
        """批量测试频道"""
        self.total_count = len(channels)
        self.success_count = 0
        
        if self.enable_logging:
            logger.info("▶️ 开始测速 | 总数: %d | 并发: %d", self.total_count, self.concurrency)
            logger.info("⚙️ 测速配置 | UDP方法: %s | HTTP方法: %s", 
                      self.udp_test_method, self.http_test_method)
            logger.info("⚙️ 速度阈值 | HTTP: %.1fKB/s | UDP: %.1fKB/s", 
                      self.min_download_speed, self.min_udp_download_speed)

        # 按IP分组并随机化
        ip_groups = self._group_channels_by_ip(channels)
        if self.enable_logging:
            logger.debug("⏳ IP分组完成 | 唯一IP数: %d", len(ip_groups))

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        ) as session:
            tasks = []
            for ip, group in ip_groups.items():
                if ip in self.blocked_ips:
                    if self.enable_logging:
                        logger.debug("⏭️ 跳过被屏蔽IP: %s", ip)
                    continue
                    
                task = self._process_ip_group(
                    session, ip, group, progress_cb, failed_urls, white_list)
                tasks.append(task)
            
            await asyncio.gather(*tasks)
        
        if self.enable_logging:
            success_rate = (self.success_count / self.total_count) * 100
            logger.info(
                "✅ 测速完成 | 成功: %d(%.1f%%) | 失败: %d | 屏蔽IP: %d",
                self.success_count, success_rate,
                self.total_count - self.success_count,
                len(self.blocked_ips)
            )

    async def _process_ip_group(self, 
                              session: aiohttp.ClientSession,
                              ip: str, 
                              channels: List[Channel],
                              progress_cb: Callable,
                              failed_urls: Set[str],
                              white_list: Set[str]) -> None:
        """处理同一IP下的频道组"""
        # IP冷却检查
        if ip in self.ip_cooldown:
            elapsed = time.time() - self.ip_cooldown[ip]
            if elapsed < self.min_ip_interval:
                wait_time = self.min_ip_interval - elapsed
                if self.enable_logging:
                    logger.debug("⏳ IP冷却中: %s | 等待 %.1f秒", ip, wait_time)
                await asyncio.sleep(wait_time)
        
        try:
            for channel in channels:
                await self._test_single_channel(
                    session, channel, progress_cb, failed_urls, white_list)
                    
            # 成功则重置失败计数
            if ip in self.failed_ips:
                del self.failed_ips[ip]
                
        except Exception as e:
            if self.enable_logging:
                logger.error("❌ IP组测试异常 | IP: %s | 错误: %s", ip, str(e))
            self.failed_ips[ip] += 1
            
            # 检查是否达到最大失败次数
            if self.failed_ips[ip] >= self.max_failures_per_ip:
                self.blocked_ips.add(ip)
                if self.enable_logging:
                    logger.warning("🛑 屏蔽IP: %s (连续失败 %d 次)", ip, self.failed_ips[ip])
        finally:
            self.ip_cooldown[ip] = time.time()

    async def _test_single_channel(self,
                                 session: aiohttp.ClientSession,
                                 channel: Channel,
                                 progress_cb: Callable,
                                 failed_urls: Set[str],
                                 white_list: Set[str]) -> None:
        """测试单个频道"""
        if self._is_in_white_list(channel, white_list):
            channel.status = 'online'
            if self.enable_logging:
                logger.debug("🟢 白名单跳过 | %s", channel.name)
            progress_cb()
            return

        async with self.semaphore:
            try:
                is_udp = bool(self.rtp_udp_pattern.search(channel.url))
                if self.enable_logging:
                    logger.debug("🔍 开始测试 | %s | %s", 
                               'UDP' if is_udp else 'HTTP', channel.name)

                if is_udp:
                    success, speed, latency = await self._test_udp_channel(session, channel)
                else:
                    success, speed, latency = await self._test_http_channel(session, channel)
                
                if success:
                    self._handle_success(channel, speed, latency, is_udp)
                else:
                    self._handle_failure(channel, failed_urls, speed, latency, is_udp)
                    
            except Exception as e:
                self._handle_error(channel, failed_urls, e)
            finally:
                progress_cb()

    async def _test_udp_channel(self,
                              session: aiohttp.ClientSession,
                              channel: Channel) -> Tuple[bool, float, float]:
        """测试UDP协议频道"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            timeout = aiohttp.ClientTimeout(total=self.udp_timeout)
            
            if self.udp_test_method == 'latency':
                # 仅测试延迟
                start = time.time()
                async with session.head(
                    channel.url,
                    headers=headers,
                    timeout=timeout
                ) as resp:
                    latency = (time.time() - start) * 1000
                    if latency > self.max_udp_latency:
                        return False, 0.0, latency
                    return (200 <= resp.status < 400), 0.0, latency
            else:
                # 测试下载速度和延迟
                start = time.time()
                async with session.get(
                    channel.url,
                    headers=headers,
                    timeout=timeout
                ) as resp:
                    latency = (time.time() - start) * 1000
                    if resp.status != 200:
                        return False, 0.0, latency
                    
                    # 读取足够的数据来测量速度
                    content = await resp.content.read(1024 * 100)  # 读取100KB数据
                    duration = time.time() - start
                    if duration == 0:
                        return False, 0.0, latency
                    
                    speed = len(content) / duration / 1024  # KB/s
                    
                    # 同时检查速度和延迟
                    speed_ok = speed >= self.min_udp_download_speed
                    latency_ok = latency <= self.max_udp_latency
                    
                    return (speed_ok and latency_ok), speed, latency
                    
        except asyncio.TimeoutError:
            return False, 0.0, self.udp_timeout * 1000
        except Exception as e:
            if self.enable_logging:
                logger.debug("UDP测试异常: %s | URL: %s", str(e), channel.url)
            return False, 0.0, 0.0

    async def _test_http_channel(self,
                              session: aiohttp.ClientSession,
                              channel: Channel) -> Tuple[bool, float, float]:
        """测试HTTP协议频道"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            timeout = aiohttp.ClientTimeout(total=self.http_timeout)
            
            # 根据配置选择测试模式
            if self.http_test_method == 'latency':
                # 仅测试延迟
                start = time.time()
                async with session.head(
                    channel.url,
                    headers=headers,
                    timeout=timeout
                ) as resp:
                    latency = (time.time() - start) * 1000
                    if latency > self.max_http_latency:
                        return False, 0.0, latency
                    return (200 <= resp.status < 400), 0.0, latency
            
            elif self.http_test_method == 'speed':
                # 仅测试速度
                start = time.time()
                async with session.get(
                    channel.url,
                    headers=headers,
                    timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        return False, 0.0, 0.0
                    
                    content = await resp.read()
                    speed = len(content) / (time.time() - start) / 1024
                    return speed >= self.min_download_speed, speed, 0.0
            
            else:
                # 测试延迟和速度
                # 阶段1: 延迟测试
                latency_start = time.time()
                async with session.head(
                    channel.url,
                    headers=headers,
                    timeout=timeout
                ) as resp:
                    latency = (time.time() - latency_start) * 1000
                    if resp.status >= 400:
                        return False, 0.0, latency
                    if latency > self.max_http_latency:
                        return False, 0.0, latency
                
                # 阶段2: 速度测试
                start = time.time()
                async with session.get(
                    channel.url,
                    headers=headers,
                    timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        return False, 0.0, latency
                    
                    content = await resp.read()
                    speed = len(content) / (time.time() - start) / 1024
                    return speed >= self.min_download_speed, speed, latency
                
        except asyncio.TimeoutError:
            return False, 0.0, self.http_timeout * 1000
        except Exception as e:
            if self.enable_logging:
                logger.debug("HTTP测试异常: %s | URL: %s", str(e), channel.url)
            return False, 0.0, 0.0

    def _handle_success(self,
                      channel: Channel,
                      speed: float,
                      latency: float,
                      is_udp: bool) -> None:
        """处理测试成功"""
        self.success_count += 1
        channel.status = 'online'
        channel.response_time = latency
        channel.download_speed = speed
        
        if self.enable_logging:
            logger.info(
                "✅ 测速成功 | 协议: %-4s | 频道: %s | 速度: %6.1fKB/s | 延迟: %4.0fms | 分类: %s | URL: %s",
                "UDP" if is_udp else "HTTP",
                channel.name,  # 不再截断
                speed,
                latency,
                channel.category,  # 不再截断
                self._simplify_url(channel.url)  # 现在返回完整URL或适度截断
            )

    def _handle_failure(self,
                       channel: Channel,
                       failed_urls: Set[str],
                       speed: float,
                       latency: float,
                       is_udp: bool) -> None:
        """处理测试失败"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        if self.enable_logging:
            reason = (
                "速度不足" if speed > 0 and speed < (self.min_udp_download_speed if is_udp else self.min_download_speed) else
                "延迟过高" if latency > (self.max_udp_latency if is_udp else self.max_http_latency) else
                "连接失败"
            )
            logger.warning(
                "❌ 测速失败 | 协议: %-4s | 频道: %s | 速度: %6.1fKB/s | 延迟: %4.0fms | 分类: %s | 原因: %-8s | URL: %s",
                "UDP" if is_udp else "HTTP",
                channel.name,
                speed,
                latency,
                channel.category,
                reason,
                self._simplify_url(channel.url)
            )

    def _handle_error(self,
                     channel: Channel,
                     failed_urls: Set[str],
                     error: Exception) -> None:
        """处理测试异常"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        if self.enable_logging:
            logger.error(
                "‼️ 测速异常 | 频道: %s | 错误: %s | URL: %s",
                channel.name,
                str(error),
                self._simplify_url(channel.url)
            )

    def _group_channels_by_ip(self, 
                           channels: List[Channel]) -> Dict[str, List[Channel]]:
        """按IP地址分组频道"""
        groups = defaultdict(list)
        for channel in channels:
            ip = self._extract_ip_from_url(channel.url)
            groups[ip].append(channel)
        return groups

    def _extract_ip_from_url(self, url: str) -> str:
        """从URL中提取IP地址"""
        try:
            parsed = urlparse(url)
            netloc = parsed.netloc.split('@')[-1]  # 处理认证信息
            if '[' in netloc and ']' in netloc:  # IPv6地址
                return netloc.split(']')[0] + ']'
            return netloc.split(':')[0]  # IPv4地址或域名
        except:
            return "unknown"

    def _simplify_url(self, url: str) -> str:
        """
        方案一：直接返回完整 URL，或者限制最大长度避免日志过长
        现在推荐直接返回完整 URL，不做截断，确保调试信息完整
        """
        max_length = 100  # 最大显示长度，超过则加 ...
        if len(url) > max_length:
            return url[:max_length] + '...'
        return url

    def _is_in_white_list(self,
                        channel: Channel,
                        white_list: Set[str]) -> bool:
        """检查频道是否在白名单中"""
        if not white_list:
            return False
        return any(
            w.lower() == channel.name.lower() or 
            w.lower() in channel.url.lower()
            for w in white_list
        )