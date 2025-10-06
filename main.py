#!/usr/bin/env python3
import os
import asyncio
import configparser
from pathlib import Path
from typing import List, Set, Dict, Optional, Tuple, Callable
import re
import logging
import gc
import sys
from datetime import datetime
from collections import defaultdict
from core import (
    SourceFetcher,
    PlaylistParser,
    AutoCategoryMatcher,
    SpeedTester,
    ResultExporter,
    Channel
)
from core.progress import SmartProgress

# ==================== 工具函数 ====================
def load_list_file(path: str) -> Set[str]:
    """加载名单文件（黑名单/白名单）"""
    file = Path(path)
    if not file.exists():
        return set()
    with open(file, 'r', encoding='utf-8') as f:
        return {line.strip().lower() for line in f if line.strip() and not line.startswith('#')}

def load_urls(path: str) -> List[str]:
    """加载订阅源URL列表"""
    file = Path(path)
    if not file.exists():
        raise FileNotFoundError(f"订阅源文件不存在: {file}")
    with open(file, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def is_blacklisted(channel: Channel, blacklist: Set[str]) -> bool:
    """检查频道是否在黑名单中"""
    channel_name = channel.name.lower()
    channel_url = channel.url.lower()
    return any(
        entry in channel_name or entry in channel_url
        for entry in blacklist
        if entry.strip() and not entry.startswith('#')
    )

async def fetch_sources(fetcher: SourceFetcher, urls: List[str], logger: logging.Logger) -> List[str]:
    """获取订阅源内容（带重试）"""
    contents = []
    for attempt in range(1, 3):
        try:
            progress = SmartProgress(len(urls), f"获取订阅源(尝试{attempt}/2)")
            contents = await fetcher.fetch_all(urls, progress.update)
            progress.complete()
            break
        except Exception as e:
            if attempt == 2:
                raise
            logger.warning(f"第{attempt}次获取失败: {str(e)}")
            await asyncio.sleep(2 ** attempt)
    return [c for c in contents if c and c.strip()]

def parse_channels(parser: PlaylistParser, contents: List[str], logger: logging.Logger) -> List[Channel]:
    """解析所有频道"""
    all_channels = []
    progress = SmartProgress(len(contents), "解析进度")
    
    for content in contents:
        try:
            channels = list(parser.parse(content))
            all_channels.extend(channels)
            
            if len(all_channels) % 5000 == 0:
                gc.collect()
                
            progress.update()
        except Exception as e:
            logger.error(f"解析异常: {str(e)}")
            continue
    
    progress.complete()
    return all_channels

def remove_duplicates(channels: List[Channel], logger: logging.Logger) -> List[Channel]:
    """去重处理"""
    progress = SmartProgress(len(channels), "去重进度")
    unique_channels = {channel.url: channel for channel in channels}
    progress.update(len(channels))
    progress.complete()
    return list(unique_channels.values())

def filter_blacklist(channels: List[Channel], blacklist: Set[str], logger: logging.Logger) -> List[Channel]:
    """黑名单过滤"""
    if not blacklist:
        return channels
        
    progress = SmartProgress(len(channels), "过滤进度")
    filtered = [c for c in channels if not is_blacklisted(c, blacklist)]
    progress.update(len(channels))
    progress.complete()
    return filtered

def classify_channels(matcher: AutoCategoryMatcher, channels: List[Channel], logger: logging.Logger) -> List[Channel]:
    """智能分类"""
    progress = SmartProgress(len(channels), "分类进度")
    
    # 批量匹配分类
    category_mapping = matcher.batch_match([c.name for c in channels])
    
    # 应用分类结果
    processed = []
    for channel in channels:
        channel.category = category_mapping[channel.name]
        channel.name = matcher.normalize_channel_name(channel.name)
        processed.append(channel)
        progress.update()
    
    progress.complete()
    return processed

async def test_channels(tester: SpeedTester, channels: List[Channel], whitelist: Set[str], logger: logging.Logger) -> Set[str]:
    """测速测试（修复Windows限制）"""
    if not channels:
        logger.warning("⚠️ 无频道需要测速")
        return set()

    failed_urls = set()
    
    # Windows系统使用更小的批次大小
    if os.name == 'nt':
        batch_size = min(200, len(channels) // 5 or 50)
    else:
        batch_size = min(1000, len(channels) // 10 or 100)
    
    progress = SmartProgress(len(channels), "测速进度")
    
    try:
        for i in range(0, len(channels), batch_size):
            batch = channels[i:i+batch_size]
            await tester.test_channels(batch, progress.update, failed_urls, whitelist)
            
            # 批处理间延迟，避免资源耗尽
            if i + batch_size < len(channels):
                await asyncio.sleep(1)
                gc.collect()
                
    except Exception as e:
        logger.error(f"测速过程异常: {str(e)}")
    finally:
        progress.complete()
    
    return failed_urls

async def export_results(exporter: ResultExporter, channels: List[Channel], whitelist: Set[str], logger: logging.Logger) -> None:
    """结果导出"""
    progress = SmartProgress(1, "导出进度")
    exporter.export(channels, whitelist, progress.update)  # 同步调用
    progress.complete()

# ==================== 主流程 ====================
def print_start_page(config: configparser.ConfigParser, logger: logging.Logger):
    """打印优化后的启动页面"""
    title = r"""
   ____   _   _   _       ___   _   _  __  __
  / ___| | \ | | | |     |_ _| | | | | \ \/ /
 | |     |  \| | | |      | |  | | | |  \  / 
 | |___  | |\  | | |___   | |  | |_| |  /  \ 
  \____| |_| \_| |_____| |___|  \___/  /_/\_\
    """
    
    # 获取关键配置
    urls_path = config.get('PATHS', 'urls_path', fallback='config/urls.txt')
    templates_path = config.get('PATHS', 'templates_path', fallback='config/templates.txt')
    output_dir = config.get('MAIN', 'output_dir', fallback='outputs')
    uncategorized_path = config.get('PATHS', 'uncategorized_channels_path', fallback='config/uncategorized.txt')
    blacklist_path = config.get('BLACKLIST', 'blacklist_path', fallback='config/blacklist.txt')
    whitelist_path = config.get('WHITELIST', 'whitelist_path', fallback='config/whitelist.txt')
    failed_urls_path = config.get('PATHS', 'failed_urls_path', fallback='config/failed_urls.txt')
    log_file_path = config.get('LOGGING', 'log_file_path', fallback='outputs/debug.log')
    
    fetcher_timeout = config.getfloat('FETCHER', 'timeout', fallback=15)
    fetcher_concurrency = config.getint('FETCHER', 'concurrency', fallback=5)
    tester_timeout = config.getfloat('TESTER', 'timeout', fallback=10)
    tester_concurrency = config.getint('TESTER', 'concurrency', fallback=8)
    tester_logging = config.getboolean('TESTER', 'enable_logging', fallback=False)
    enable_history = config.getboolean('EXPORTER', 'enable_history', fallback=False)
    log_level = config.get('LOGGING', 'log_level', fallback='INFO').upper()
    
    # 获取版本信息
    try:
        from core import __version__
        version = f"v{__version__}"
    except ImportError:
        version = "v1.0.0"
    
    # 优化后的启动信息
    start_info = f"""
{title}
╔══════════════════════════════════════════════════════╗
║  IPTV智能处理系统 {version.ljust(36)}  ║
║  启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S').ljust(43)}║
╟──────────────────────────────────────────────────────╢
║  处理流程:                                            ║
║   1. 获取订阅源 → 2. 解析频道 → 3. 去重              ║
║   4. 黑名单过滤 → 5. 智能分类 → 6. 测速测试          ║
║   7. 结果导出 → 8. 完成!                             ║
╟──────────────────────────────────────────────────────╢
║  核心配置概览:                                        ║
║  • 订阅源路径: {urls_path.ljust(46)}║
║  • 分类模板: {templates_path.ljust(48)}║
║  • 未分类频道路径: {uncategorized_path.ljust(41)}║
║  • 黑名单路径: {blacklist_path.ljust(45)}║
║  • 白名单路径: {whitelist_path.ljust(45)}║
║  • 失败URL路径: {failed_urls_path.ljust(43)}║
║  • 日志文件路径: {log_file_path.ljust(42)}║
║  • 输出目录: {output_dir.ljust(46)}║
║  • 抓取并发数: {str(fetcher_concurrency).ljust(3)} 超时: {str(fetcher_timeout).ljust(4)}秒          ║
║  • 测速并发数: {str(tester_concurrency).ljust(3)} 超时: {str(tester_timeout).ljust(4)}秒          ║
║  • 测速日志: {'启用' if tester_logging else '禁用'.ljust(45)}║
║  • 历史记录: {'启用' if enable_history else '禁用'.ljust(45)}║
║  • 日志级别: {log_level.ljust(46)}║
╚══════════════════════════════════════════════════════╝
"""
    logger.info(start_info)

def setup_logging(config: configparser.ConfigParser) -> logging.Logger:
    """配置日志系统"""
    logger = logging.getLogger()
    logger.setLevel(config.get('LOGGING', 'log_level', fallback='INFO').upper())

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.addHandler(console_handler)

    if config.getboolean('LOGGING', 'log_to_file', fallback=False):
        log_file = Path(config.get('LOGGING', 'log_file_path', fallback='outputs/debug.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        logger.addHandler(file_handler)
    
    return logger

def increase_file_limit():
    """尝试提高Windows文件描述符限制"""
    if os.name == 'nt':
        try:
            import ctypes
            # 尝试提高限制到2048
            ctypes.windll.ws2_32.WSASetMaxSocketCount(2048)
            logging.getLogger().info("成功提高Windows socket限制")
        except Exception:
            logging.getLogger().warning("无法提高Windows socket限制，使用保守配置")

async def main():
    """主工作流程（完整修复版）"""
    try:
        # ==================== 初始化阶段 ====================
        print("="*60)
        config = configparser.ConfigParser()
        config.read('config/config.ini', encoding='utf-8')
        logger = setup_logging(config)
        
        # 尝试提高文件限制
        increase_file_limit()
        
        logger.info("✅ 配置加载完成")
        print_start_page(config, logger)

        # ==================== 数据准备阶段 ====================
        logger.info("\n🔹🔹🔹🔹 阶段1/7：数据准备")
        blacklist = load_list_file(config.get('BLACKLIST', 'blacklist_path', fallback='config/blacklist.txt'))
        whitelist = load_list_file(config.get('WHITELIST', 'whitelist_path', fallback='config/whitelist.txt'))
        urls = load_urls(config.get('PATHS', 'urls_path', fallback='config/urls.txt'))
        logger.info(f"• 加载黑名单: {len(blacklist)}条")
        logger.info(f"• 加载白名单: {len(whitelist)}条")
        logger.info(f"• 加载订阅源: {len(urls)}个")

        # ==================== 订阅源获取阶段 ====================
        logger.info("\n🔹🔹🔹🔹 阶段2/7：获取订阅源")
        fetcher = SourceFetcher(
            timeout=config.getfloat('FETCHER', 'timeout', fallback=15),
            concurrency=config.getint('FETCHER', 'concurrency', fallback=5),
            config=config
        )
        contents = await fetch_sources(fetcher, urls, logger)
        logger.info(f"✅ 获取完成 | 成功: {len(contents)}/{len(urls)}")

        # ==================== 频道解析阶段 ====================
        logger.info("\n🔹🔹🔹🔹 阶段3/7：解析频道")
        parser = PlaylistParser(config)
        all_channels = parse_channels(parser, contents, logger)
        unique_sources = len({c.url for c in all_channels})
        logger.info(f"✅ 解析完成 | 总频道: {len(all_channels)} | 唯一源: {unique_sources}")

        # ==================== 数据处理阶段 ====================
        logger.info("\n🔹🔹🔹🔹 阶段4/7：数据处理")
        unique_channels = remove_duplicates(all_channels, logger)
        filtered_channels = filter_blacklist(unique_channels, blacklist, logger)
        logger.info(f"✔ 处理完成 | 去重后: {len(unique_channels)} | 过滤后: {len(filtered_channels)}")

        # ==================== 智能分类阶段 ====================
        logger.info("\n🔹🔹🔹🔹 阶段5/7：智能分类")
        matcher = AutoCategoryMatcher(
            config.get('PATHS', 'templates_path', fallback='config/templates.txt'),
            config
        )
        processed_channels = classify_channels(matcher, filtered_channels, logger)
        classified = sum(1 for c in processed_channels if c.category != "未分类")
        logger.info(f"✅ 分类完成 | 已分类: {classified} | 未分类: {len(processed_channels)-classified}")

        # ==================== 测速测试阶段 ====================
        logger.info("\n🔹🔹🔹🔹 阶段6/7：测速测试")
        tester = SpeedTester(
            timeout=config.getfloat('TESTER', 'timeout', fallback=10),
            concurrency=config.getint('TESTER', 'concurrency', fallback=8),
            max_attempts=config.getint('TESTER', 'max_attempts', fallback=2),
            min_download_speed=config.getfloat('TESTER', 'min_download_speed', fallback=0.1),
            enable_logging=config.getboolean('TESTER', 'enable_logging', fallback=False),
            config=config
        )
        sorted_channels = matcher.sort_channels_by_template(processed_channels, whitelist)
        failed_urls = await test_channels(tester, sorted_channels, whitelist, logger)
        online_count = sum(1 for c in sorted_channels if c.status == 'online')
        logger.info(f"✅ 测速完成 | 在线: {online_count}/{len(sorted_channels)} | 失败: {len(failed_urls)}")

        # ==================== 结果导出阶段 ====================
        logger.info("\n🔹🔹🔹🔹 阶段7/7：结果导出")
        exporter = ResultExporter(
            output_dir=config.get('MAIN', 'output_dir', fallback='outputs'),
            template_path=config.get('PATHS', 'templates_path'),
            config=config,
            matcher=matcher
        )
        await export_results(exporter, sorted_channels, whitelist, logger)

        # ==================== 最终统计 ====================
        logger.info("\n" + "="*60)
        logger.info("📊📊 最终统计")
        logger.info(f"• 总处理频道: {len(sorted_channels)}")
        logger.info(f"• 在线频道: {online_count} (成功率: {online_count/len(sorted_channels)*100:.1f}%)")
        logger.info(f"• 未分类频道: {len(processed_channels)-classified}")
        logger.info("="*60 + "\n🎉🎉 任务完成！")

    except KeyboardInterrupt:
        logger.error("\n🛑🛑🛑 用户中断操作")
        sys.exit(0)
    except Exception as e:
        logger.error("\n" + "‼️"*20)
        logger.error(f"致命错误: {str(e)}", exc_info=True)
        logger.error("‼️"*20)
        sys.exit(1)

if __name__ == "__main__":
    # Windows系统设置事件循环策略
    if os.name == 'nt':
        from asyncio import WindowsSelectorEventLoopPolicy
        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

    # 初始化日志（临时用于配置加载）
    logging.basicConfig(level=logging.INFO)
    temp_logger = logging.getLogger()

    try:
        # 加载配置
        config = configparser.ConfigParser()
        config.read('config/config.ini', encoding='utf-8')
        
        # 重新配置日志
        logger = setup_logging(config)
        
        # 运行主程序
        asyncio.run(main())
    except Exception as e:
        temp_logger.error(f"启动失败: {str(e)}", exc_info=True)
        sys.exit(1)