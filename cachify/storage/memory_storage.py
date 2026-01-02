import contextlib
import time
from typing import Any

from cachify.types import CacheEntry, Number

_CACHE_CLEAR_INTERVAL_SECONDS: int = 10


class MemoryCacheEntry(CacheEntry): ...


class MemoryStorage:
    """In-memory cache storage implementing CacheStorage protocol."""

    _CACHE: dict[str, MemoryCacheEntry] = {}

    @classmethod
    def clear_expired_cached_items(cls):
        """Clear expired cached items from the cache."""
        while True:
            with contextlib.suppress(Exception):
                for key, entry in list(cls._CACHE.items()):
                    if entry.is_expired():
                        del cls._CACHE[key]

            time.sleep(_CACHE_CLEAR_INTERVAL_SECONDS)

    @classmethod
    def set(cls, cache_key: str, result: Any, ttl: Number | None):
        cls._CACHE[cache_key] = MemoryCacheEntry(result, ttl)

    @classmethod
    def get(cls, cache_key: str, skip_cache: bool) -> MemoryCacheEntry | None:
        if skip_cache:
            return None
        if entry := cls._CACHE.get(cache_key):
            if not entry.is_expired():
                return entry
        return None

    @classmethod
    async def aset(cls, cache_key: str, result: Any, ttl: Number | None):
        cls.set(cache_key, result, ttl)

    @classmethod
    async def aget(cls, cache_key: str, skip_cache: bool) -> MemoryCacheEntry | None:
        return cls.get(cache_key, skip_cache)

    @classmethod
    def clear(cls):
        cls._CACHE.clear()
