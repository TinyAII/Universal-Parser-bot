import asyncio
import json
from re import Match
from typing import ClassVar
from urllib.parse import urlencode

import httpx

from astrbot.api import logger
from astrbot.core.config.astrbot_config import AstrBotConfig

from ..data import ImageContent, Platform, VideoContent, Author, ParseResult
from ..exception import ParseException
from ..utils import generate_file_name
from .base import BaseParser, Downloader, handle


class ShortVideoParser(BaseParser):
    """使用第三方API的短视频解析器，支持抖音、快手等全网短视频平台"""
    
    # 平台信息
    platform: ClassVar[Platform] = Platform(name="shortvideo", display_name="短视频")
    
    # 标记为备用解析器，不需要在配置中启用
    is_backup_parser = True
    
    def __init__(self, config: AstrBotConfig, downloader: Downloader):
        super().__init__(config, downloader)
        # 第三方API配置
        self.api_url = "https://api.mymzf.com/api/shortvideo"
        self.api_headers = {
            "User-Agent": "xiaoxiaoapi/1.0.0"
        }
        # 创建HTTP客户端
        self.http_client = httpx.AsyncClient(headers=self.api_headers, timeout=30)
    
    async def close_session(self) -> None:
        """关闭HTTP客户端"""
        await super().close_session()
        await self.http_client.aclose()
    
    @handle("v.douyin.com", r"v\.douyin\.com/[A-Za-z0-9]+/")
    @handle("v.kuaishou.com", r"v\.kuaishou\.com/[A-Za-z0-9]+")
    @handle("xhslink.com", r"xhslink\.com/[A-Za-z0-9]+")
    @handle("m.toutiao.com", r"m\.toutiao\.com/is/[A-Za-z0-9]+/")
    @handle("weibo.com", r"weibo\.com/[0-9]+/[A-Za-z0-9]+/")
    @handle("b23.tv", r"b23\.tv/[A-Za-z0-9]+/")
    @handle("douyin.com", r"douyin\.com/video/[0-9]+")
    async def _parse_short_video(self, searched: Match[str]):
        """解析短视频链接"""
        # 获取完整URL
        full_url = searched.group(0)
        if not full_url.startswith("http"):
            full_url = f"https://{full_url}"
        
        logger.debug(f"开始解析短视频链接: {full_url}")
        
        # 调用第三方API解析
        try:
            api_response = await self.http_client.get(
                self.api_url, 
                params={"url": full_url}
            )
            api_response.raise_for_status()
        except httpx.HTTPError as e:
            logger.exception(f"调用第三方API失败: {e}")
            raise ParseException(f"调用短视频解析API失败: {str(e)}")
        
        # 解析API返回结果
        try:
            result = api_response.json()
            logger.debug(f"API返回结果: {json.dumps(result, ensure_ascii=False)[:100]}...")
            
            if result["code"] != 200:
                raise ParseException(f"短视频解析失败: {result.get('msg', '未知错误')}")
            
            data = result["data"]
        except (json.JSONDecodeError, KeyError) as e:
            logger.exception(f"解析API返回结果失败: {e}")
            raise ParseException(f"解析API返回结果失败: {str(e)}")
        
        # 提取作者信息
        author_name = data.get("author") or data.get("auther") or "未知作者"
        author_avatar = data.get("avatar")
        
        author = self.create_author(
            name=author_name,
            avatar_url=author_avatar
        )
        
        # 提取视频信息
        title = data.get("title")
        cover_url = data.get("cover")
        video_url = data.get("url")
        
        if not video_url:
            raise ParseException("未找到视频下载链接")
        
        # 下载视频
        async def download_video_task():
            file_name = generate_file_name(video_url, ".mp4")
            return await self.downloader.streamd(
                video_url,
                file_name=file_name,
                ext_headers={"Referer": full_url}
            )
        
        # 创建视频封面下载任务
        cover_task = None
        if cover_url:
            cover_task = self.downloader.download_img(cover_url)
        
        # 创建视频内容
        video_content = self.create_video_content(
            asyncio.create_task(download_video_task()),
            cover_task,
            0.0  # API未返回时长，默认为0
        )
        
        # 提取图片内容（如果有）
        contents = [video_content]
        images = data.get("images", [])
        for img_url in images:
            img_task = self.downloader.download_img(img_url)
            contents.append(ImageContent(img_task))
        
        # 构建解析结果
        return self.result(
            url=full_url,
            title=title,
            author=author,
            contents=contents,
            extra={"info": f"使用第三方API解析"}
        )


__all__ = ["ShortVideoParser"]
