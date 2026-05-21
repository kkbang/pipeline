from collections import deque
from dataclasses import dataclass
from time import monotonic


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int
    reset_after_seconds: int


class FixedWindowRateLimiter:
    def __init__(self, *, limit: int, window_seconds: int) -> None:
        self.limit = max(1, int(limit))
        self.window_seconds = max(1, int(window_seconds))
        self._events_by_key: dict[str, deque[float]] = {}

    def check(self, key: str, *, now: float | None = None) -> RateLimitDecision:
        normalized_key = str(key or "").strip() or "unknown"
        current_time = float(monotonic() if now is None else now)
        window_start = current_time - self.window_seconds
        bucket = self._events_by_key.setdefault(normalized_key, deque())

        while bucket and bucket[0] <= window_start:
            bucket.popleft()

        current_count = len(bucket)
        if current_count >= self.limit:
            oldest_in_window = bucket[0]
            retry_after = max(1, int((oldest_in_window + self.window_seconds) - current_time))
            return RateLimitDecision(
                allowed=False,
                limit=self.limit,
                remaining=0,
                retry_after_seconds=retry_after,
                reset_after_seconds=retry_after,
            )

        bucket.append(current_time)
        remaining = max(0, self.limit - len(bucket))
        if bucket:
            reset_after = max(0, int((bucket[0] + self.window_seconds) - current_time))
        else:
            reset_after = self.window_seconds
        return RateLimitDecision(
            allowed=True,
            limit=self.limit,
            remaining=remaining,
            retry_after_seconds=0,
            reset_after_seconds=reset_after,
        )
