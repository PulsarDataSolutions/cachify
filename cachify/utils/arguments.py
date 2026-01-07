import inspect
from collections.abc import Callable, Generator
from inspect import Signature
from typing import Any

from cachify.types import CacheKeyFunction
from cachify.utils.errors import CacheKeyError
from cachify.utils.functions import get_function_id
from cachify.utils.hash import object_hash


def _iter_arguments(
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
    function: Callable[..., Any],
    cache_key_func: CacheKeyFunction | None,
    ignore_fields: tuple[str, ...],
    args: tuple,
    kwargs: dict,
) -> str:
    function_id = get_function_id(function)

    if not cache_key_func:
        function_signature = inspect.signature(function)
        items = tuple(_iter_arguments(function_signature, args, kwargs, ignore_fields))
        return f"{function_id}:{object_hash(items)}"

    cache_key = cache_key_func(args, kwargs)
    try:
        return f"{function_id}:{object_hash(cache_key)}"
    except TypeError as exc:
        raise CacheKeyError(
            "Cache key function must return a hashable cache key - be careful with mutable types (list, dict, set) and non built-in types"
        ) from exc
