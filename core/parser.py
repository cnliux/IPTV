import re
from typing import Generator, List
import logging
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from .models import Channel
from functools import lru_cache

logger = logging.getLogger(__name__)

class PlaylistParser:
    """M3U解析器（支持源分类保留）"""
    
    CHANNEL_REGEX = re.compile(r'^(.*?),(http.*)$', re.MULTILINE)
    EXTINF_REGEX = re.compile(
        r'#EXTINF:-?[\d.]*,?(.*?)(?:\s+tvg-name="([^"]*)")?(?:\s+tvg-logo="([^"]*)")?(?:\s+group-title="([^"]*)")?.*\n(.*)',
        re.IGNORECASE
    )
    GROUP_TITLE_REGEX = re.compile(r'group-title="([^"]+)"')
    TVG_NAME_REGEX = re.compile(r'tvg-name="([^"]+)"')
    TVG_LOGO_REGEX = re.compile(r'tvg-logo="([^"]+)"')
    
    def __init__(self, config=None):
        self.config = config
        self.params_to_remove = set()
        if config and config.has_section('URL_FILTER'):
            params = config.get('URL_FILTER', 'remove_params', fallback='')
            self.params_to_remove = {p.strip() for p in params.split(',') if p.strip()}

    def parse(self, content: str) -> Generator[Channel, None, None]:
        """解析内容生成频道列表（保留原始分类）"""
        lines = content.splitlines()
        batch_size = min(1000, len(lines) // 10 or 100)
        
        current_category = None
        current_extinf = None
        for i in range(0, len(lines), batch_size):
            batch = lines[i:i+batch_size]
            for channel in self._parse_batch(batch, current_category, current_extinf):
                current_category = channel.original_category
                yield channel

    def _parse_batch(self, batch: List[str], current_category: str, current_extinf: str) -> Generator[Channel, None, None]:
        """解析内容批次（带分类提取）"""
        channel_matches = []
        
        for line in batch:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('#EXTINF'):
                current_extinf = line
                # 从EXTINF行提取group-title
                if match := self.GROUP_TITLE_REGEX.search(line):
                    current_category = match.group(1)
                elif match := self.EXTINF_REGEX.match(line):
                    if match.group(4):  # group-title from EXTINF_REGEX
                        current_category = match.group(4)
            elif current_extinf and line.startswith('http'):
                # 处理完整的EXTINF + URL组合
                if match := self.EXTINF_REGEX.match(current_extinf):
                    name = match.group(2) or match.group(1)  # 优先使用tvg-name
                    logo = match.group(3)
                    group_title = match.group(4) or current_category
                    
                    channel_matches.append((
                        name.strip() if name else self._clean_name(current_extinf),
                        line,
                        group_title or "未分类",
                        logo
                    ))
                else:
                    channel_matches.append((
                        self._clean_name(current_extinf),
                        line,
                        current_category or "未分类",
                        None
                    ))
                current_extinf = None
            else:
                if match := self.CHANNEL_REGEX.match(line):
                    channel_matches.append((match.group(1), match.group(2), current_category, None))
                elif match := self.EXTINF_REGEX.match(line):
                    name = match.group(2) or match.group(1)
                    channel_matches.append((
                        name.strip() if name else self._clean_name(line),
                        match.group(5),
                        match.group(4) or current_category or "未分类",
                        match.group(3)
                    ))

        for name, url, category, logo in channel_matches:
            channel = Channel(
                name=self._clean_name(name),
                url=self._clean_url(url),
                original_category=category or "未分类"  # 确保始终有分类
            )
            if logo:
                channel.logo = logo
            yield channel

    def _clean_name(self, raw_name: str) -> str:
        """清理频道名称（保留原始名称）"""
        # 处理EXTINF行中的名称
        if raw_name.startswith('#EXTINF'):
            if match := re.search(r'#EXTINF:-?\d+,(.*)', raw_name):
                return match.group(1).strip()
            return raw_name.split(',')[-1].strip()
        
        # 处理普通名称
        return raw_name.split(',')[-1].strip()

    def _clean_url(self, raw_url: str) -> str:
        """清理URL（带参数过滤）"""
        url = raw_url.split('$')[0].strip()
        
        if self.params_to_remove:
            try:
                parsed = urlparse(url)
                if parsed.query:
                    query_params = parse_qs(parsed.query, keep_blank_values=True)
                    filtered_params = {k: v for k, v in query_params.items() if k not in self.params_to_remove}
                    new_query = urlencode(filtered_params, doseq=True)
                    url = urlunparse(parsed._replace(query=new_query))
            except Exception as e:
                logger.warning(f"URL参数处理失败: {url}, 错误: {str(e)}")
        
        return url