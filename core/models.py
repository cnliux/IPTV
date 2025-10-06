import re
from typing import ClassVar

class Channel:
    """频道数据模型（内存优化版）"""
    __slots__ = ['name', 'url', 'category', 'original_category', 
                'status', 'response_time', 'download_speed']

    # 类变量（静态变量）定义
    IPV4_PATTERN: ClassVar[re.Pattern] = re.compile(
        r'https?://(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})(?::\d+)?'
    )
    
    IPV6_PATTERN: ClassVar[re.Pattern] = re.compile(
        r'https?://(?:\[[0-9a-fA-F:]+\]|[0-9a-fA-F]*:[0-9a-fA-F:]+)'
    )

    def __init__(self, 
                 name: str, 
                 url: str, 
                 category: str = "未分类",
                 original_category: str = "未分类",
                 status: str = "pending",
                 response_time: float = 0.0,
                 download_speed: float = 0.0):
        self.name = name
        self.url = url
        self.category = category
        self.original_category = original_category
        self.status = status
        self.response_time = response_time
        self.download_speed = download_speed

    @classmethod
    def classify_ip_type(cls, url: str) -> str:
        """分类IP类型: ipv4 或 ipv6"""
        return "ipv6" if cls.IPV6_PATTERN.search(url) else "ipv4"