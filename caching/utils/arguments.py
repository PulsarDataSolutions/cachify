import hashlib
import pickle
from collections.abc import Generator
from inspect import Signature
from typing import Any

from caching.types import CacheKeyFunction


def _cache_key_fingerprint(value: object) -> str:
    payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    return hashlib.blake2b(payload, digest_size=16).hexdigest()


def iter_arguments(
    function_signature: Signature,
    args: tuple,
    kwargs: dict,
    ignore_fields: tuple[str, ...],
) -> Generator[Any, None, None]:
    bound = function_signature.bind_partial(*args, **kwargs)
    bound.apply_defaults()

    for name, value in bound.arguments.items():
        if name in ignore_fields:
            continue

        param = function_signature.parameters[name]

        # Positional variable arguments can just be yielded like so
        if param.kind == param.VAR_POSITIONAL:
            yield from value
            continue

        # Keyword variable arguments need to be unpacked from .items()
        if param.kind == param.VAR_KEYWORD:
            yield from value.items()
            continue

        yield name, value


def create_cache_key(
    function_signature: Signature,
    cache_key_func: CacheKeyFunction | None,
    ignore_fields: tuple[str, ...],
    args: tuple,
    kwargs: dict,
) -> str:
    if not cache_key_func:
        items = tuple(iter_arguments(function_signature, args, kwargs, ignore_fields))
        return _cache_key_fingerprint(items)

    cache_key = cache_key_func(args, kwargs)
    try:
        return _cache_key_fingerprint(cache_key)
    except TypeError as exc:
        raise ValueError(
            "Cache key function must return a hashable cache key - be careful with mutable types (list, dict, set) and non built-in types"
        ) from exc
