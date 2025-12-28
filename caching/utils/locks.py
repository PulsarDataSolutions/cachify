import asyncio
import threading
from collections import defaultdict

ASYNC_LOCKS: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
SYNC_LOCKS: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)
