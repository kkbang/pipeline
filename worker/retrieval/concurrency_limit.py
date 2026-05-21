import asyncio
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ConcurrencyDecision:
    allowed: bool
    limit: int
    in_flight: int
    remaining: int


class ConcurrencyReservation:
    def __init__(self, limiter: "NonBlockingConcurrencyLimiter") -> None:
        self._limiter = limiter
        self._released = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._limiter._release()

    async def __aenter__(self) -> "ConcurrencyReservation":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()


class NonBlockingConcurrencyLimiter:
    def __init__(self, *, limit: int) -> None:
        self.limit = max(1, int(limit))
        self._lock = asyncio.Lock()
        self._in_flight = 0

    async def try_acquire(self) -> tuple[ConcurrencyDecision, ConcurrencyReservation | None]:
        async with self._lock:
            current_in_flight = self._in_flight
            if current_in_flight >= self.limit:
                return (
                    ConcurrencyDecision(
                        allowed=False,
                        limit=self.limit,
                        in_flight=current_in_flight,
                        remaining=0,
                    ),
                    None,
                )

            self._in_flight += 1
            updated_in_flight = self._in_flight

        return (
            ConcurrencyDecision(
                allowed=True,
                limit=self.limit,
                in_flight=updated_in_flight,
                remaining=max(0, self.limit - updated_in_flight),
            ),
            ConcurrencyReservation(self),
        )

    async def _release(self) -> None:
        async with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1
