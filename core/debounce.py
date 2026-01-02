# debounce.py

import time

from astrbot.core.config.astrbot_config import AstrBotConfig


class LinkDebouncer:
    """
    会话级链接防抖器。
    用法：
        debouncer = LinkDebouncer(interval=10)
        if debouncer.hit(session_id, link):   # 最近出现过
            return
        # 真正处理
    """

    def __init__(self, config: AstrBotConfig):
        self.interval = config["debounce_interval"]
        self._cache: dict[str, dict[str, float]] = {}  # {session: {link: ts}}

    def hit(self, session: str, link: str) -> bool:
        """返回 True 表示命中防抖，应跳过"""
        now = time.time()
        bucket = self._cache.setdefault(session, {})

        # 1. 清理过期
        for k, ts in list(bucket.items()):
            if now - ts > self.interval:
                bucket.pop(k, None)

        # 2. 检查是否已存在
        if link in bucket:
            return True

        # 3. 记录本次时间
        bucket[link] = now
        return False
