"""
Abuse limits for the gateway. Everything here is in memory and per process (fine for one gateway container; a
scaled-out deployment would move it to Redis).

- RateLimiter     token bucket per key: a burst is allowed, then a steady rate. Used per device.
- FailureLimiter  counts failed logins/tokens per client in a sliding window. Used to stop token guessing.
- ReplayCache     remembers recent (device, message_id) pairs so a captured message cannot be sent again.
"""
import threading
import time
from collections import OrderedDict, defaultdict, deque

_lock = threading.Lock()


class RateLimiter:
    def __init__(self, burst: int, per_minute: int) -> None:
        self.burst, self.rate = burst, per_minute / 60.0
        self.buckets: dict[str, tuple[float, float]] = {}   # key -> (tokens, last time)

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with _lock:
            tokens, last = self.buckets.get(key, (float(self.burst), now))
            tokens = min(self.burst, tokens + (now - last) * self.rate)
            ok = tokens >= 1
            self.buckets[key] = (tokens - 1 if ok else tokens, now)
            if len(self.buckets) > 50_000:               # never let an attacker grow this without bound
                self.buckets.pop(next(iter(self.buckets)))
            return ok


class FailureLimiter:
    def __init__(self, max_failures: int, window_s: int) -> None:
        self.max, self.window = max_failures, window_s
        self.hits: dict[str, deque] = defaultdict(deque)

    def _trim(self, key: str, now: float) -> deque:
        q = self.hits[key]
        while q and now - q[0] > self.window:
            q.popleft()
        return q

    def blocked(self, key: str) -> bool:
        with _lock:
            return len(self._trim(key, time.monotonic())) >= self.max

    def fail(self, key: str) -> None:
        with _lock:
            now = time.monotonic()
            self._trim(key, now).append(now)
            if len(self.hits) > 50_000:
                self.hits.pop(next(iter(self.hits)))


class ReplayCache:
    def __init__(self, ttl_s: int, max_items: int = 200_000) -> None:
        self.ttl, self.max = ttl_s, max_items
        self.seen: OrderedDict[tuple, float] = OrderedDict()

    def is_replay(self, key: tuple) -> bool:
        now = time.monotonic()
        with _lock:
            while self.seen and (now - next(iter(self.seen.values())) > self.ttl or len(self.seen) > self.max):
                self.seen.popitem(last=False)
            return key in self.seen

    def remember(self, key: tuple) -> None:
        with _lock:
            self.seen[key] = time.monotonic()


import os  # noqa: E402  (kept next to the instances so the settings are easy to find)

device_rate = RateLimiter(int(os.getenv("RATE_BURST", "60")), int(os.getenv("RATE_PER_MIN", "120")))
auth_failures = FailureLimiter(int(os.getenv("AUTH_MAX_FAILURES", "10")), int(os.getenv("AUTH_FAILURE_WINDOW_S", "60")))
replays = ReplayCache(int(os.getenv("REPLAY_TTL_S", "900")))


def reset() -> None:
    """Forget everything (used by tests and the pen-test script)."""
    with _lock:
        device_rate.buckets.clear(); auth_failures.hits.clear(); replays.seen.clear()