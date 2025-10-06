import logging
from pathlib import Path
from datetime import datetime
from typing import List, Callable, Set, Dict, Tuple, Optional
from .models import Channel
import csv
from urllib.parse import quote
import re
from collections import defaultdict
import gzip
import shutil

logger = logging.getLogger(__name__)

class ResultExporter:
    """增强版结果导出器（修复M3U拼接问题）"""

    def __init__(self, 
                output_dir: str, 
                template_path: str, 
                config, 
                matcher):
        """
        初始化导出器
        参数:
            output_dir: 输出目录路径
            template_path: 分类模板路径
            config: 配置对象
            matcher: 分类匹配器实例
        """
        self.output_dir = Path(output_dir)
        self.template_path = template_path
        self.config = config
        self.matcher = matcher
        self.uncategorized_path = Path(config.get(
            'PATHS', 
            'uncategorized_channels_path', 
            fallback=str(self.output_dir / "uncategorized.txt")
        ))
        self._ensure_dirs()

    def _ensure_dirs(self):
        """确保所有输出目录存在"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.uncategorized_path.parent.mkdir(parents=True, exist_ok=True)
        Path(self.config.get('PATHS', 'failed_urls_path', fallback='config/failed_urls.txt')).parent.mkdir(exist_ok=True)
        Path(self.config.get('PATHS', 'csv_output_path', fallback=str(self.output_dir / "history"))).mkdir(exist_ok=True)

    def export(self, 
              channels: List[Channel], 
              whitelist: Set[str],
              progress_cb: Callable) -> None:
        """
        核心导出方法（同步）
        参数:
            channels: 已排序的频道列表
            whitelist: 白名单集合
            progress_cb: 进度回调函数
        """
        try:
            # 按原始分组收集未分类频道
            uncategorized = defaultdict(list)
            for channel in channels:
                if channel.category == "未分类":
                    clean_name = self.matcher.normalize_channel_name(channel.name)
                    uncategorized[channel.original_category].append((clean_name, channel.url))

            # 导出主文件（自动跳过未分类）
            valid_channels = [c for c in channels if c.category != "未分类"]
            self._export_all(valid_channels)
            
            # 导出协议分类文件
            ipv4, ipv6 = self._classify_channels(valid_channels)
            self._export_channels(ipv4, "ipv4")
            self._export_channels(ipv6, "ipv6")
            
            # 导出未分类频道（按原始分组）
            if uncategorized:
                self._export_uncategorized(uncategorized)
            
            # 历史记录（包含所有频道）
            if self.config.getboolean('EXPORTER', 'enable_history', fallback=False):
                self._export_history(channels)
            
            progress_cb(1)
        except Exception as e:
            logger.error(f"导出过程中发生错误: {str(e)}", exc_info=True)
            raise

    def _export_uncategorized(self, uncategorized: Dict[str, List[Tuple[str, str]]]) -> None:
        """专用未分类频道导出"""
        try:
            with open(self.uncategorized_path, 'w', encoding='utf-8') as f:
                for original_category in sorted(uncategorized.keys()):
                    channels = uncategorized[original_category]
                    if not channels:
                        continue
                        
                    f.write(f"{original_category},#genre#\n")
                    for name, url in sorted(channels, key=lambda x: x[0].lower()):
                        f.write(f"{name},{url}\n")
                    f.write("\n")
            
            logger.info(
                f"未分类频道已保存 | 文件: {self.uncategorized_path} | "
                f"分组数: {len(uncategorized)} | "
                f"频道数: {sum(len(v) for v in uncategorized.values())}"
            )
        except Exception as e:
            logger.error(f"未分类频道导出失败: {str(e)}", exc_info=True)

    def _classify_channels(self, channels: List[Channel]) -> Tuple[List[Channel], List[Channel]]:
        """分类频道为IPv4/IPv6（已跳过未分类频道）"""
        ipv4_channels = [c for c in channels if c.status == 'online' and Channel.classify_ip_type(c.url) != "ipv6"]
        ipv6_channels = [c for c in channels if c.status == 'online' and Channel.classify_ip_type(c.url) == "ipv6"]
        return ipv4_channels, ipv6_channels

    def _export_all(self, channels: List[Channel]) -> None:
        """主文件导出（确保不含未分类频道）"""
        m3u_path = self.output_dir / self.config.get('EXPORTER', 'm3u_filename', fallback='all.m3u')
        txt_path = self.output_dir / self.config.get('EXPORTER', 'txt_filename', fallback='all.txt')
        
        m3u_count = self._export_m3u(channels, m3u_path)
        txt_count = self._export_txt(channels, txt_path)
        
        logger.info(
            f"主文件导出完成 | M3U: {m3u_path} ({m3u_count}频道) | "
            f"TXT: {txt_path} ({txt_count}频道)"
        )

    def _export_m3u(self, channels: List[Channel], file_path: Path) -> int:
        """M3U格式导出（修复版）"""
        online_channels = [c for c in channels if c.status == 'online']
        
        with open(file_path, 'w', encoding='utf-8') as f:
            # 写入M3U文件头
            f.write(self._get_m3u_header())
            
            # 按分类分组频道
            channels_by_category = defaultdict(list)
            for channel in online_channels:
                channels_by_category[channel.category].append(channel)
            
            # 按模板顺序写入分类
            for category in self._get_template_order():
                if category in channels_by_category:
                    category_channels = channels_by_category[category]
                    for channel in category_channels:
                        self._write_m3u_channel(f, channel)
            
            # 写入其他分类（按字母顺序）
            other_categories = sorted([cat for cat in channels_by_category.keys() 
                                     if cat not in self._get_template_order()])
            for category in other_categories:
                for channel in channels_by_category[category]:
                    self._write_m3u_channel(f, channel)
        
        return len(online_channels)

    def _write_m3u_channel(self, file_obj, channel: Channel) -> None:
        """写入单个频道到M3U文件（修复格式问题）"""
        try:
            # 生成台标URL
            logo_url = self.config.get('EXPORTER', 'm3u_logo_url', fallback='').format(
                name=quote(channel.name),
                category=quote(channel.category)
            )
            
            # 清理频道名称（移除可能影响格式的特殊字符）
            clean_name = re.sub(r'[\r\n]', '', channel.name)
            clean_category = re.sub(r'[\r\n]', '', channel.category)
            
            # 写入EXTINF行
            extinf_line = f'#EXTINF:-1 tvg-name="{clean_name}" group-title="{clean_category}" tvg-logo="{logo_url}",{clean_name}\n'
            file_obj.write(extinf_line)
            
            # 写入URL行
            file_obj.write(f"{channel.url}\n")
            
        except Exception as e:
            logger.warning(f"写入频道失败 {channel.name}: {str(e)}")

    def _get_template_order(self) -> List[str]:
        """获取模板中的分类顺序"""
        try:
            order = []
            with open(self.template_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.endswith(',#genre#'):
                        category = line.split(',')[0]
                        order.append(category)
            return order
        except Exception:
            return []

    def _export_txt(self, channels: List[Channel], file_path: Path) -> int:
        """TXT格式导出（兼容传统播放器）"""
        seen_urls = set()
        current_category = None
        count = 0
        
        with open(file_path, 'w', encoding='utf-8') as f:
            # 按分类分组
            channels_by_category = defaultdict(list)
            for channel in channels:
                if channel.status == 'online' and channel.url not in seen_urls:
                    seen_urls.add(channel.url)
                    channels_by_category[channel.category].append(channel)
            
            # 按模板顺序写入分类
            for category in self._get_template_order():
                if category in channels_by_category:
                    f.write(f"{category},#genre#\n")
                    for channel in channels_by_category[category]:
                        f.write(f"{channel.name},{channel.url}\n")
                        count += 1
                    f.write("\n")
            
            # 写入其他分类（按字母顺序）
            other_categories = sorted([cat for cat in channels_by_category.keys() 
                                     if cat not in self._get_template_order()])
            for category in other_categories:
                f.write(f"{category},#genre#\n")
                for channel in channels_by_category[category]:
                    f.write(f"{channel.name},{channel.url}\n")
                    count += 1
                f.write("\n")
                
        return count

    def _export_channels(self, channels: List[Channel], type_name: str) -> None:
        """协议专用文件导出（IPv4/IPv6）"""
        output_txt = Path(self.config.get('PATHS', f'{type_name}_output_path', fallback=f'{type_name}.txt'))
        output_m3u = output_txt.with_suffix('.m3u')

        txt_count = self._export_txt(channels, self.output_dir / output_txt)
        m3u_count = self._export_m3u(channels, self.output_dir / output_m3u)
        
        logger.info(
            f"{type_name.upper()}频道导出完成 | "
            f"TXT: {output_txt} ({txt_count}频道) | "
            f"M3U: {output_m3u} ({m3u_count}频道)"
        )

    def _export_history(self, channels: List[Channel]) -> None:
        """历史记录导出（含所有频道状态）"""
        csv_output_path = Path(self.config.get(
            'PATHS', 
            'csv_output_path', 
            fallback=str(self.output_dir / "history")
        ))
        csv_output_path.mkdir(parents=True, exist_ok=True)
        history_file = csv_output_path / f"history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            with open(history_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Name', 'URL', 'Category', 'OriginalCategory',
                    'Status', 'Speed(KB/s)', 'Response(ms)'
                ])
                for ch in channels:
                    writer.writerow([
                        ch.name, ch.url, ch.category, ch.original_category,
                        ch.status, ch.download_speed, ch.response_time
                    ])
            
            if self.config.getboolean('EXPORTER', 'compress_history', fallback=True):
                with open(history_file, 'rb') as f_in:
                    with gzip.open(f"{history_file}.gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                history_file.unlink()
                logger.info(f"历史记录已压缩: {history_file}.gz | 总频道: {len(channels)}")
            else:
                logger.info(f"历史记录已保存: {history_file} | 总频道: {len(channels)}")
        except Exception as e:
            logger.error(f"历史记录导出失败: {str(e)}")

    def _get_m3u_header(self) -> str:
        """生成M3U文件头（从配置读取EPG地址）"""
        epg_url = self.config.get(
            'EXPORTER', 
            'm3u_epg_url', 
            fallback='http://epg.51zmt.top:8000/cc.xml.gz'
        )
        return (
            f'#EXTM3U x-tvg-url="{epg_url}" '
            'catchup="append" '
            'catchup-source="?playseek=${{(b)yyyyMMddHHmmss}}-${{(e)yyyyMMddHHmmss}}"\n'
        )