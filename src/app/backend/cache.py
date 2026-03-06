"""Simple TTL cache for API responses.

Designed for demo environments where underlying data changes infrequently.
Cache entries are keyed on the full set of function arguments and expire
after a configurable TTL.
"""

import threading
import time
from functools import wraps
from typing import Any, Callable


def ttl_cache(seconds: int = 60) -> Callable:
    """Decorator that caches function return values with a time-to-live.

    Unlike functools.lru_cache, keys are built from all args/kwargs
    (including unhashable defaults handled via repr) and entries expire.
    Thread-safe via a per-function lock.
    """

    def decorator(func: Callable) -> Callable:
        _cache: dict[str, tuple[float, Any]] = {}
        _lock = threading.Lock()

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = f"{args!r}|{sorted(kwargs.items())!r}"
            now = time.monotonic()

            with _lock:
                entry = _cache.get(key)
                if entry and (now - entry[0]) < seconds:
                    return entry[1]

            result = func(*args, **kwargs)

            with _lock:
                _cache[key] = (time.monotonic(), result)

                if len(_cache) > 100:
                    expired = [k for k, (ts, _) in _cache.items() if time.monotonic() - ts >= seconds]
                    for k in expired:
                        del _cache[k]

            return result

        wrapper.cache_clear = lambda: _cache.clear()  # type: ignore[attr-defined]
        return wrapper

    return decorator
