import hashlib
import pickle
from typing import Any

from cachify.utils.errors import CacheKeyError


def object_hash(value: Any) -> str:
    try:
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    except Exception as exc:
        raise CacheKeyError(
            "Unable to serialize object for hashing - ensure all parts of the object are pickleable. "
            "Hint: create a custom __reduce__ method for the suspected object if necessary."
        ) from exc

    return hashlib.blake2b(payload, digest_size=16).hexdigest()
