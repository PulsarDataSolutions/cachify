import pickle
import time
from typing import Any

from caching.redis.config import get_redis_config
from caching.config import logger
from caching.types import CacheEntry, Number


class RedisCacheEntry(CacheEntry):
    @classmethod
    def time(cls) -> float:
        return time.time()


class RedisStorage:
    """Redis cache storage implementing CacheStorage protocol."""

    @classmethod
    def _make_key(cls, function_id: str, cache_key: str) -> str:
        """Create a Redis key from function_id and cache_key."""
        config = get_redis_config()
        return f"{config.key_prefix}:{function_id}:{cache_key}"

    @classmethod
    def _serialize(cls, entry: RedisCacheEntry) -> bytes:
        """Serialize a cache entry to bytes."""
        try:
            return pickle.dumps(entry, protocol=pickle.HIGHEST_PROTOCOL)
        except (pickle.PicklingError, TypeError, AttributeError) as exc:
            raise TypeError(
                f"Failed to serialize cache entry. Object of type {type(entry.result).__name__} "
                f"cannot be pickled. Ensure the cached result is serializable."
            ) from exc

    @classmethod
    def _deserialize(cls, data: bytes) -> RedisCacheEntry:
        """Deserialize bytes to a cache entry."""
        return pickle.loads(data)

    @classmethod
    def _require_sync_client(cls):
        """Raise if sync client not configured."""
        config = get_redis_config()
        if config.sync_client is None:
            raise RuntimeError(
                "Redis sync client not configured. "
                "Provide sync_client in setup_redis_config() to use @redis_cache on sync functions."
            )

    @classmethod
    def _require_async_client(cls):
        """Raise if async client not configured."""
        config = get_redis_config()
        if config.async_client is None:
            raise RuntimeError(
                "Redis async client not configured. "
                "Provide async_client in setup_redis_config() to use @redis_cache on async functions."
            )

    @classmethod
    def _prepare_set(
        cls, function_id: str, cache_key: str, result: Any, ttl: Number | None
    ) -> tuple[str, bytes, int | None]:
        """Prepare key, data, and expiry for set operations."""
        key = cls._make_key(function_id, cache_key)
        entry = RedisCacheEntry(result, ttl)
        data = cls._serialize(entry)
        expiry = int(ttl) + 1 if ttl is not None else None
        return key, data, expiry

    @classmethod
    def _handle_error(cls, exc: Exception, operation: str) -> None:
        """Handle Redis errors based on config."""
        config = get_redis_config()
        if config.on_error == "raise":
            raise
        logger.debug(f"Redis {operation} error (silent mode): {exc}")

    @classmethod
    def set(cls, function_id: str, cache_key: str, result: Any, ttl: Number | None):
        """Store a result in Redis cache."""
        cls._require_sync_client()
        config = get_redis_config()
        key, data, expiry = cls._prepare_set(function_id, cache_key, result, ttl)
        try:
            if expiry is None:
                config.sync_client.set(key, data)
            else:
                config.sync_client.setex(key, expiry, data)
        except Exception as exc:
            cls._handle_error(exc, "set")

    @classmethod
    def get(cls, function_id: str, cache_key: str, skip_cache: bool) -> RedisCacheEntry | None:
        """Retrieve a cache entry from Redis."""
        if skip_cache:
            return None

        cls._require_sync_client()
        config = get_redis_config()
        key = cls._make_key(function_id, cache_key)
        try:
            data = config.sync_client.get(key)
            if data is None:
                return None

            entry = cls._deserialize(bytes(data))  # type: ignore[arg-type]
            if entry.is_expired():
                return None
            return entry
        except Exception as exc:
            cls._handle_error(exc, "get")
            return None

    @classmethod
    def is_expired(cls, function_id: str, cache_key: str) -> bool:
        """Check if a cache entry is expired or doesn't exist."""
        cls._require_sync_client()
        config = get_redis_config()
        key = cls._make_key(function_id, cache_key)
        try:
            data = config.sync_client.get(key)
            if data is None:
                return True

            entry = cls._deserialize(bytes(data))  # type: ignore[arg-type]
            return entry.is_expired()
        except Exception as exc:
            cls._handle_error(exc, "is_expired")
            return True

    @classmethod
    async def aset(cls, function_id: str, cache_key: str, result: Any, ttl: Number | None):
        """Store a result in Redis cache (async)."""
        cls._require_async_client()
        config = get_redis_config()
        key, data, expiry = cls._prepare_set(function_id, cache_key, result, ttl)
        try:
            if expiry is None:
                await config.async_client.set(key, data)
            else:
                await config.async_client.setex(key, expiry, data)
        except Exception as exc:
            cls._handle_error(exc, "aset")

    @classmethod
    async def aget(cls, function_id: str, cache_key: str, skip_cache: bool) -> RedisCacheEntry | None:
        """Retrieve a cache entry from Redis (async)."""
        if skip_cache:
            return None

        cls._require_async_client()
        config = get_redis_config()
        key = cls._make_key(function_id, cache_key)
        try:
            data = await config.async_client.get(key)
            if data is None:
                return None

            entry = cls._deserialize(bytes(data))  # type: ignore[arg-type]
            if entry.is_expired():
                return None
            return entry
        except Exception as exc:
            cls._handle_error(exc, "aget")
            return None

    @classmethod
    async def ais_expired(cls, function_id: str, cache_key: str) -> bool:
        """Check if a cache entry is expired or doesn't exist (async)."""
        cls._require_async_client()
        config = get_redis_config()
        key = cls._make_key(function_id, cache_key)
        try:
            data = await config.async_client.get(key)
            if data is None:
                return True

            entry = cls._deserialize(bytes(data))  # type: ignore[arg-type]
            return entry.is_expired()
        except Exception as exc:
            cls._handle_error(exc, "ais_expired")
            return True
