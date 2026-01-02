import re
from random import choice
from typing import ClassVar, TypeAlias

import msgspec
from msgspec import Struct, field

from astrbot.api import logger
from astrbot.core.config.astrbot_config import AstrBotConfig

from ..data import Platform
from ..download import Downloader
from .base import BaseParser, ParseException, handle


class KuaiShouParser(BaseParser):
    """快手解析器"""

    # 平台信息
    platform: ClassVar[Platform] = Platform(name="kuaishou", display_name="快手")
    
    # 固定CDN节点列表
    KUAISHOU_CDN_NODES = [
        "corp.kuaishou.com",
        "creator.kuaishou.com",
        "e.kuaishou.com",
        "m.kuaishou.com"
    ]

    def __init__(self, config: AstrBotConfig, downloader: Downloader):
        super().__init__(config, downloader)
        self.ios_headers["Referer"] = "https://v.kuaishou.com/"

    # https://v.kuaishou.com/2yAnzeZ
    @handle("v.kuaishou", r"v\.kuaishou\.com/[A-Za-z\d._?%&+\-=/#]+")
    # https://www.kuaishou.com/short-video/3xhjgcmir24m4nm
    @handle("kuaishou", r"(?:www\.)?kuaishou\.com/[A-Za-z\d._?%&+\-=/#]+")
    # https://v.m.chenzhongtech.com/fw/photo/3xburnkmj3auazc
    @handle("chenzhongtech", r"(?:v\.m\.)?chenzhongtech\.com/fw/[A-Za-z\d._?%&+\-=/#]+")
    async def _parse_v_kuaishou(self, searched: re.Match[str]):
        # 从匹配对象中获取原始URL
        url = f"https://{searched.group(0)}"
        
        # 增加页面获取的重试机制
        max_retries = 3
        real_url = ""
        for attempt in range(max_retries):
            try:
                real_url = await self.get_redirect_url(url, headers=self.ios_headers)
                if len(real_url) > 0:
                    break
            except Exception as e:
                logger.warning(f"获取真实URL失败，尝试重试... | 尝试 {attempt+1}/{max_retries} | 错误: {str(e)}")
                await asyncio.sleep(1)
        
        if len(real_url) <= 0:
            raise ParseException("failed to get location url from url after multiple retries")

        # /fw/long-video/ 返回结果不一样, 统一替换为 /fw/photo/ 请求
        real_url = real_url.replace("/fw/long-video/", "/fw/photo/")
        
        # 增加页面内容获取的重试机制
        response_text = ""
        for attempt in range(max_retries):
            try:
                async with self.client.get(real_url, headers=self.ios_headers, timeout=30) as resp:
                    if resp.status >= 400:
                        raise ParseException(f"获取页面失败 {resp.status}")
                    response_text = await resp.text()
                if response_text:
                    break
            except Exception as e:
                logger.warning(f"获取页面内容失败，尝试重试... | 尝试 {attempt+1}/{max_retries} | 错误: {str(e)}")
                await asyncio.sleep(1)
        
        if not response_text:
            raise ParseException("failed to get page content after multiple retries")

        pattern = r"window\.INIT_STATE\s*=\s*(.*?)</script>"
        matched = re.search(pattern, response_text)

        if not matched:
            raise ParseException("failed to parse video JSON info from HTML")

        json_str = matched.group(1).strip()
        init_state = msgspec.json.decode(json_str, type=KuaishouInitState)
        photo = next((d.photo for d in init_state.values() if d.photo is not None), None)
        if photo is None:
            raise ParseException("window.init_state don't contains videos or pics")

        # 简洁的构建方式
        contents = []

        # 添加视频内容
        if video_url := photo.video_url:
            contents.append(self.create_video_content(video_url, photo.cover_url, photo.duration))

        # 添加图片内容
        if img_urls := photo.img_urls:
            contents.extend(self.create_image_contents(img_urls))

        # 构建作者
        author = self.create_author(photo.name, photo.head_url)

        return self.result(
            title=photo.caption,
            author=author,
            contents=contents,
            timestamp=photo.timestamp // 1000,
        )





class CdnUrl(Struct):
    cdn: str
    url: str | None = None


class Atlas(Struct):
    music_cdn_list: list[CdnUrl] = field(name="musicCdnList", default_factory=list)
    cdn_list: list[CdnUrl] = field(name="cdnList", default_factory=list)
    size: list[dict] = field(name="size", default_factory=list)
    img_route_list: list[str] = field(name="list", default_factory=list)

    @property
    def img_urls(self):
        if len(self.cdn_list) == 0 or len(self.img_route_list) == 0:
            return []
        cdn = choice(self.cdn_list).cdn
        return [f"https://{cdn}/{url}" for url in self.img_route_list]


class ExtParams(Struct):
    atlas: Atlas = field(default_factory=Atlas)


class Photo(Struct):
    # 标题
    caption: str
    timestamp: int
    duration: int = 0
    user_name: str = field(default="未知用户", name="userName")
    head_url: str | None = field(default=None, name="headUrl")
    cover_urls: list[CdnUrl] = field(name="coverUrls", default_factory=list)
    main_mv_urls: list[CdnUrl] = field(name="mainMvUrls", default_factory=list)
    ext_params: ExtParams = field(name="ext_params", default_factory=ExtParams)

    @property
    def name(self) -> str:
        return self.user_name.replace("\u3164", "").strip()

    @property
    def cover_url(self):
        """返回所有可用的封面URL列表，而非单个随机URL"""
        return [cdn_url.url for cdn_url in self.cover_urls if cdn_url.url]

    @property
    def video_url(self):
        """返回所有可用的视频URL列表，而非单个随机URL"""
        return [cdn_url.url for cdn_url in self.main_mv_urls if cdn_url.url]

    @property
    def img_urls(self):
        """返回所有可用的图片URL列表，使用固定CDN节点"""
        atlas = self.ext_params.atlas
        if not atlas.img_route_list:
            return []
        
        # 使用固定的CDN节点列表
        from . import KuaiShouParser
        
        # 为每个图片路径生成所有CDN节点的URL
        all_img_urls = []
        for img_route in atlas.img_route_list:
            for cdn_node in KuaiShouParser.KUAISHOU_CDN_NODES:
                img_url = f"https://{cdn_node}/{img_route}"
                all_img_urls.append(img_url)
        
        return all_img_urls


class TusjohData(Struct):
    result: int
    photo: Photo | None = None



KuaishouInitState: TypeAlias = dict[str, TusjohData]
