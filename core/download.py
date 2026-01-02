import asyncio
from asyncio import Task, create_task
from collections.abc import Callable, Coroutine
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import aiofiles
import yt_dlp
from aiohttp import ClientError, ClientSession, ClientTimeout
from msgspec import Struct, convert
from tqdm.asyncio import tqdm

from astrbot.api import logger
from astrbot.core.config.astrbot_config import AstrBotConfig

from .constants import COMMON_HEADER
from .exception import (
    DownloadException,
    DurationLimitException,
    ParseException,
    SizeLimitException,
    ZeroSizeException,
)
from .utils import LimitedSizeDict, generate_file_name, merge_av, safe_unlink

P = ParamSpec("P")
T = TypeVar("T")


def auto_task(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Task[T]]:
    """装饰器：自动将异步函数调用转换为 Task, 完整保留类型提示"""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Task[T]:
        coro = func(*args, **kwargs)
        name = " | ".join(str(arg) for arg in args if isinstance(arg, str))
        return create_task(coro, name=func.__name__ + " | " + name)

    return wrapper


class VideoInfo(Struct):
    title: str
    """标题"""
    channel: str
    """频道名称"""
    uploader: str
    """上传者 id"""
    duration: int
    """时长"""
    timestamp: int
    """发布时间戳"""
    thumbnail: str
    """封面图片"""
    description: str
    """简介"""
    channel_id: str
    """频道 id"""

    @property
    def author_name(self) -> str:
        return f"{self.channel}@{self.uploader}"


class Downloader:
    """下载器，支持youtube-dlp 和 流式下载"""

    def __init__(self, config: AstrBotConfig):
        self.config = config
        self.cache_dir = Path(config["cache_dir"])
        self.proxy: str | None = self.config["proxy"] or None
        self.max_duration: int = config["source_max_minute"] * 60
        self.max_size = self.config["source_max_size"]
        self.headers: dict[str, str] = COMMON_HEADER.copy()
        # 视频信息缓存
        self.info_cache: LimitedSizeDict[str, VideoInfo] = LimitedSizeDict()
        # 用于流式下载的客户端
        # 优化超时设置：将连接超时和下载超时分开
        self.client = ClientSession(
            timeout=ClientTimeout(
                total=config["download_timeout"],
                connect=30,  # 连接超时设为30秒，增加B站CDN连接成功概率
                sock_connect=30,  # socket连接超时设为30秒
                sock_read=300  # socket读取超时设为300秒，适应大文件下载
            )
        )
    @auto_task
    async def streamd(
        self,
        url: str | list[str],
        *,
        file_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
        proxy: str | None | object = ...,
        retry_count: int = 5,  # 增加重试次数到5次，提高B站CDN连接成功率
    ) -> Path:
        """download file by url with stream

        Args:
            url (str | list[str]): url address or list of fallback urls
            file_name (str | None): file name. Defaults to generate_file_name.
            ext_headers (dict[str, str] | None): ext headers. Defaults to None.
            proxy (str | None): proxy URL. Defaults to configured proxy. Use None to disable proxy.
            retry_count (int): 重试次数. Defaults to 3.

        Returns:
            Path: file path

        Raises:
            httpx.HTTPError: When download fails
        """
        # 如果是URL列表，依次尝试每个URL
        if isinstance(url, list):
            for i, single_url in enumerate(url):
                try:
                    return await self.streamd(
                        single_url,
                        file_name=file_name,
                        ext_headers=ext_headers,
                        proxy=proxy,
                        retry_count=1  # 单个URL只重试1次，失败则尝试下一个
                    )
                except Exception as e:
                    logger.warning(f"URL {single_url} 下载失败，尝试下一个... | 尝试 {i+1}/{len(url)} | 错误: {str(e)}")
            # 所有URL都失败
            raise DownloadException("所有URL都下载失败")
        

        if not file_name:
            file_name = generate_file_name(url)
        file_path = self.cache_dir / file_name
        # 如果文件存在，则直接返回
        if file_path.exists():
            return file_path

        headers = {**self.headers, **(ext_headers or {})}
        
        # Use sentinel value to detect if proxy was explicitly passed
        if proxy is ...:
            proxy = self.proxy
        
        # 针对B站CDN优化：如果是B站CDN链接，尝试使用不同的CDN节点
        is_bili_cdn = "upos-" in url and "bilivideo.com" in url or "upos-hz-mirrorakam.akamaized.net" in url
        available_nodes = []
        cdn_node_pattern = r'(upos-[a-z]+-[a-z0-9]+(?:\.[a-z0-9]+)*\.bilivideo\.com|upos-[a-z]+-[a-z0-9]+\.akamaized\.net)'
        
        if is_bili_cdn:
            # 为B站CDN添加额外的请求头
            headers["Origin"] = "https://www.bilibili.com"
            headers["Referer"] = "https://www.bilibili.com/"
            headers["Sec-Fetch-Dest"] = "empty"
            headers["Sec-Fetch-Mode"] = "cors"
            headers["Sec-Fetch-Site"] = "cross-site"
            
            # B站CDN节点列表
            bili_cdn_nodes = [
                "upos-sz-mirror08c.bilivideo.com",
                "upos-sz-mirrorcoso1.bilivideo.com",
                "upos-sz-mirrorhw.bilivideo.com",
                "upos-sz-mirror08h.bilivideo.com",
                "upos-sz-mirrorcos.bilivideo.com",
                "upos-sz-mirrorcosb.bilivideo.com",
                "upos-sz-mirrorali.bilivideo.com",
                "upos-sz-mirroralib.bilivideo.com",
                "upos-sz-mirroraliov.bilivideo.com",
                "upos-sz-mirrorcosov.bilivideo.com",
                "upos-hz-mirrorakam.akamaized.net",
                "upos-sz-mirrorcf1ov.bilivideo.com"
            ]
            
            # 提取原始URL中的CDN节点
            import re
            match = re.search(cdn_node_pattern, url)
            if match:
                original_cdn_node = match.group(0)
                logger.debug(f"原始CDN节点: {original_cdn_node}")
                # 从节点列表中移除原始节点，避免重复尝试
                available_nodes = [node for node in bili_cdn_nodes if node != original_cdn_node]
                # 将原始节点放在列表末尾，最后尝试
                available_nodes.append(original_cdn_node)
            else:
                available_nodes = bili_cdn_nodes
        
        for attempt in range(retry_count):
            try:
                # 如果是B站CDN链接，并且还有可用节点，尝试替换CDN节点
                current_url = url
                use_node = None
                if is_bili_cdn and available_nodes:
                    # 计算当前尝试使用的节点索引
                    node_index = attempt % len(available_nodes)
                    use_node = available_nodes[node_index]
                    # 替换URL中的CDN节点
                    current_url = re.sub(cdn_node_pattern, use_node, url)
                    logger.debug(f"尝试CDN节点 | 节点: {use_node} | 第 {node_index + 1}/{len(available_nodes)} 个节点")
                
                logger.debug(f"开始下载 | 尝试 {attempt + 1}/{retry_count} | url: {current_url[:100]}...")
                async with self.client.get(
                    current_url, headers=headers, allow_redirects=True, proxy=proxy
                ) as response:
                    logger.debug(f"下载响应 | 状态码: {response.status} | url: {current_url[:100]}...")
                    if response.status >= 400:
                        # 针对B站特定错误码的处理
                        if is_bili_cdn and response.status in [403, 404, 500, 502, 503, 504]:
                            logger.warning(f"B站CDN返回错误 | 状态码: {response.status} | 节点: {use_node} | url: {current_url[:100]}...")
                            # 尝试清除可能的缓存或使用不同的请求策略
                            await asyncio.sleep(1)
                            continue
                        raise ClientError(
                            f"HTTP {response.status} {response.reason}"
                        )
                    content_length = response.headers.get("Content-Length")
                    content_length = int(content_length) if content_length else 0

                    if content_length == 0:
                        logger.warning(f"媒体 url: {url}, 大小为 0, 取消下载")
                        raise ZeroSizeException
                    if (file_size := content_length / 1024 / 1024) > self.max_size:
                        logger.warning(
                            f"媒体 url: {url} 大小 {file_size:.2f} MB 超过 {self.max_size} MB, 取消下载"
                        )
                        raise SizeLimitException

                    logger.debug(f"开始流式下载 | 文件大小: {content_length} 字节 | url: {url}")
                    with self.get_progress_bar(file_name, content_length) as bar:
                        async with aiofiles.open(file_path, "wb") as file:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                await file.write(chunk)
                                bar.update(len(chunk))
                    logger.debug(f"下载完成 | 文件路径: {file_path} | url: {url}")
                    return file_path

            except ClientError as e:
                await safe_unlink(file_path)
                logger.exception(f"下载失败 | 尝试 {attempt + 1}/{retry_count} | url: {url}, file_path: {file_path}")
                # 针对B站CDN的特殊处理
                if is_bili_cdn:
                    logger.warning(f"B站CDN连接失败，尝试重试... | 错误: {str(e)}")
                    # 为B站CDN增加额外的重试间隔
                    await asyncio.sleep(3 ** attempt)
                else:
                    # 其他情况使用指数退避
                    await asyncio.sleep(2 ** attempt)
                if attempt + 1 >= retry_count:
                    if is_bili_cdn:
                        raise DownloadException(f"B站媒体下载失败: {str(e)}。可能是CDN节点问题或网络限制")
                    else:
                        raise DownloadException(f"媒体下载失败: {str(e)}")
        return file_path

    @staticmethod
    def get_progress_bar(desc: str, total: int | None = None) -> tqdm:
        """获取进度条 bar

        Args:
            desc (str): 描述
            total (int | None): 总大小. Defaults to None.

        Returns:
            tqdm: 进度条
        """
        return tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            dynamic_ncols=True,
            colour="green",
            desc=desc,
        )

    @auto_task
    async def download_video(
        self,
        url: str | list[str],
        *,
        video_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
        use_ytdlp: bool = False,
        cookiefile: Path | None = None,
        proxy: str | None | object = ...,
    ) -> Path:
        """download video file by url with stream

        Args:
            url (str | list[str]): url address or list of fallback urls
            video_name (str | None): video name. Defaults to get name by parse url.
            ext_headers (dict[str, str] | None): ext headers. Defaults to None.
            use_ytdlp (bool): use ytdlp to download video. Defaults to False.
            cookiefile (Path | None): cookie file path. Defaults to None.
            proxy (str | None): proxy URL. Defaults to configured proxy. Use None to disable proxy.

        Returns:
            Path: video file path

        Raises:
            httpx.HTTPError: When download fails
        """
        if use_ytdlp:
            # ytdlp只支持单个URL
            if isinstance(url, list):
                url = url[0]
            return await self._ytdlp_download_video(url, cookiefile)

        # 生成文件名，只使用第一个URL
        if video_name is None:
            if isinstance(url, list):
                video_name = generate_file_name(url[0], ".mp4")
            else:
                video_name = generate_file_name(url, ".mp4")
        return await self.streamd(url, file_name=video_name, ext_headers=ext_headers, proxy=proxy)

    @auto_task
    async def download_audio(
        self,
        url: str | list[str],
        *,
        audio_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
        use_ytdlp: bool = False,
        cookiefile: Path | None = None,
        proxy: str | None | object = ...,
    ) -> Path:
        """download audio file by url with stream

        Args:
            url (str | list[str]): url address or list of fallback urls
            audio_name (str | None ): audio name. Defaults to generate from url.
            ext_headers (dict[str, str] | None): ext headers. Defaults to None.
            proxy (str | None): proxy URL. Defaults to configured proxy. Use None to disable proxy.

        Returns:
            Path: audio file path

        Raises:
            httpx.HTTPError: When download fails
        """
        if use_ytdlp:
            # ytdlp只支持单个URL
            if isinstance(url, list):
                url = url[0]
            return await self._ytdlp_download_audio(url, cookiefile)

        # 生成文件名，只使用第一个URL
        if audio_name is None:
            if isinstance(url, list):
                audio_name = generate_file_name(url[0], ".mp3")
            else:
                audio_name = generate_file_name(url, ".mp3")
        return await self.streamd(url, file_name=audio_name, ext_headers=ext_headers, proxy=proxy)

    @auto_task
    async def download_file(
        self,
        url: str | list[str],
        *,
        file_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
        proxy: str | None | object = ...,
    ) -> Path:
        """download file by url with stream
        
        Args:
            url (str | list[str]): url address or list of fallback urls
            file_name (str | None): file name. Defaults to None.
            ext_headers (dict[str, str] | None): ext headers. Defaults to None.
            proxy (str | None): proxy URL. Defaults to configured proxy. Use None to disable proxy.

        Returns:
            Path: file path
        """
        # 生成文件名，只使用第一个URL
        if file_name is None:
            if isinstance(url, list):
                file_name = generate_file_name(url[0], ".zip")
            else:
                file_name = generate_file_name(url, ".zip")
        return await self.streamd(url, file_name=file_name, ext_headers=ext_headers, proxy=proxy)

    @auto_task
    async def download_img(
        self,
        url: str | list[str],
        *,
        img_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
        proxy: str | None | object = ...,
    ) -> Path:
        """download image file by url with stream

        Args:
            url (str | list[str]): url or list of fallback urls
            img_name (str | None): image name. Defaults to generate from url.
            ext_headers (dict[str, str] | None): ext headers. Defaults to None.
            proxy (str | None): proxy URL. Defaults to configured proxy. Use None to disable proxy.

        Returns:
            Path: image file path

        Raises:
            httpx.HTTPError: When download fails
        """
        # 生成文件名，只使用第一个URL
        if img_name is None:
            if isinstance(url, list):
                img_name = generate_file_name(url[0], ".jpg")
            else:
                img_name = generate_file_name(url, ".jpg")
        return await self.streamd(url, file_name=img_name, ext_headers=ext_headers, proxy=proxy)

    async def download_imgs_without_raise(
        self,
        urls: list[str],
        *,
        ext_headers: dict[str, str] | None = None,
        proxy: str | None | object = ...,
    ) -> list[Path]:
        """download images without raise

        Args:
            urls (list[str]): urls
            ext_headers (dict[str, str] | None): ext headers. Defaults to None.
            proxy (str | None): proxy URL. Defaults to configured proxy. Use None to disable proxy.

        Returns:
            list[Path]: image file paths
        """
        paths_or_errs = await asyncio.gather(
            *[self.download_img(url, ext_headers=ext_headers, proxy=proxy) for url in urls],
            return_exceptions=True,
        )
        return [p for p in paths_or_errs if isinstance(p, Path)]

    @auto_task
    async def download_av_and_merge(
        self,
        v_url: str,
        a_url: str,
        *,
        output_path: Path,
        ext_headers: dict[str, str] | None = None,
        proxy: str | None | object = ...,
        retry_count: int = 2,
    ) -> Path:
        """download video and audio file by url with stream and merge
        
        Args:
            v_url (str): video url
            a_url (str): audio url
            output_path (Path): output file path
            ext_headers (dict[str, str] | None): ext headers. Defaults to None.
            proxy (str | None): proxy URL. Defaults to configured proxy. Use None to disable proxy.
            retry_count (int): 重试次数. Defaults to 2.

        Returns:
            Path: merged file path
        """
        for attempt in range(retry_count):
            try:
                v_path, a_path = await asyncio.gather(
                    self.download_video(v_url, ext_headers=ext_headers, proxy=proxy),
                    self.download_audio(a_url, ext_headers=ext_headers, proxy=proxy),
                )
                await merge_av(v_path=v_path, a_path=a_path, output_path=output_path)
                return output_path
            except Exception as e:
                logger.exception(f"音视频合并失败 | 尝试 {attempt + 1}/{retry_count} | 视频url: {v_url}, 音频url: {a_url}")
                if attempt + 1 >= retry_count:
                    raise DownloadException(f"音视频合并失败: {str(e)}")
                # 重试前等待一段时间
                await asyncio.sleep(2 ** attempt)
        return output_path

    # region -------------------- 私有：yt-dlp --------------------

    async def ytdlp_extract_info(
        self, url: str, cookiefile: Path | None = None
    ) -> VideoInfo:
        if (info := self.info_cache.get(url)) is not None:
            return info
        opts = {
            "quiet": True,
            "skip_download": True,
            "force_generic_extractor": True,
            "cookiefile": None,
        }
        if self.proxy:
            opts["proxy"] = self.proxy
        if cookiefile and cookiefile.is_file():
            opts["cookiefile"] = str(cookiefile)
        with yt_dlp.YoutubeDL(opts) as ydl:
            raw = await asyncio.to_thread(ydl.extract_info, url, download=False)
            if not raw:
                raise ParseException("获取视频信息失败")
        info = convert(raw, VideoInfo)
        self.info_cache[url] = info
        return info

    async def _ytdlp_download_video(
        self, url: str, cookiefile: Path | None = None
    ) -> Path:
        info = await self.ytdlp_extract_info(url, cookiefile)
        if info.duration > self.max_duration:
            raise DurationLimitException

        video_path = self.cache_dir / generate_file_name(url, ".mp4")
        if video_path.exists():
            return video_path

        opts = {
            "outtmpl": str(video_path),
            "merge_output_format": "mp4",
            # "format": f"bv[filesize<={info.duration // 10 + 10}M]+ba/b[filesize<={info.duration // 8 + 10}M]",
            "format": "best[height<=720]/bestvideo[height<=720]+bestaudio/best",
            "postprocessors": [
                {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}
            ],
            "cookiefile": None,
        }
        if self.proxy:
            opts["proxy"] = self.proxy
        if cookiefile and cookiefile.is_file():
            opts["cookiefile"] = str(cookiefile)

        with yt_dlp.YoutubeDL(opts) as ydl:
            await asyncio.to_thread(ydl.download, [url])
        return video_path

    async def _ytdlp_download_audio(self, url: str, cookiefile: Path | None) -> Path:
        file_name = generate_file_name(url)
        audio_path = self.cache_dir / f"{file_name}.flac"
        if audio_path.exists():
            return audio_path

        opts = {
            "outtmpl": str(self.cache_dir / file_name) + ".%(ext)s",
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "flac",
                    "preferredquality": "0",
                }
            ],
            "cookiefile": None,
        }
        if self.proxy:
            opts["proxy"] = self.proxy
        if cookiefile and cookiefile.is_file():
            opts["cookiefile"] = str(cookiefile)

        with yt_dlp.YoutubeDL(opts) as ydl:
            await asyncio.to_thread(ydl.download, [url])
        return audio_path

    async def close(self):
        """关闭网络客户端"""
        await self.client.close()
