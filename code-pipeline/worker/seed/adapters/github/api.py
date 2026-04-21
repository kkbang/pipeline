import asyncio
import logging
import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import httpx

from worker.common.config import settings


logger = logging.getLogger(__name__)


def _parse_int(raw_value: str | None) -> int | None:
    if raw_value is None:
        return None
    try:
        return int(str(raw_value).strip())
    except (TypeError, ValueError):
        return None


def _parse_retry_after_seconds(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None

    normalized = str(raw_value).strip()
    if not normalized:
        return None

    try:
        return max(0.0, float(normalized))
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, dt.timestamp() - time.time())
    except Exception:
        return None


@dataclass(slots=True)
class _BucketRateLimitState:
    remaining: int | None = None
    reset_at_epoch: float | None = None
    retry_after_until: float = 0.0
    primary_exhausted_until: float = 0.0
    secondary_cooldown_until: float = 0.0
    recent_rate_limited_count: int = 0


class _SharedRateLimitState:
    def __init__(self) -> None:
        self._states: dict[str, _BucketRateLimitState] = defaultdict(_BucketRateLimitState)
        self._lock = threading.Lock()

    def next_delay_seconds(self, bucket: str) -> float:
        now = time.time()
        with self._lock:
            state = self._states[bucket]
            blocked_until = max(
                state.retry_after_until,
                state.primary_exhausted_until,
                state.secondary_cooldown_until,
            )
            return max(0.0, blocked_until - now)

    def apply_headers(self, bucket: str, headers: httpx.Headers) -> None:
        now = time.time()
        remaining = _parse_int(headers.get("X-RateLimit-Remaining"))
        reset_at = _parse_int(headers.get("X-RateLimit-Reset"))
        retry_after_seconds = _parse_retry_after_seconds(headers.get("Retry-After"))

        with self._lock:
            state = self._states[bucket]
            if remaining is not None:
                state.remaining = remaining
            if reset_at is not None:
                state.reset_at_epoch = float(reset_at)
            if retry_after_seconds is not None:
                state.retry_after_until = max(
                    state.retry_after_until,
                    now + retry_after_seconds,
                )
            if (
                state.remaining == 0
                and state.reset_at_epoch is not None
                and state.reset_at_epoch > now
            ):
                state.primary_exhausted_until = max(
                    state.primary_exhausted_until,
                    state.reset_at_epoch,
                )

    def mark_rate_limited(self, bucket: str, headers: httpx.Headers, retry_count: int) -> None:
        self.apply_headers(bucket, headers)
        now = time.time()
        jitter_seconds = random.uniform(
            0.0,
            max(0.0, settings.github_bucket_cooldown_jitter_sec),
        )

        with self._lock:
            state = self._states[bucket]
            state.recent_rate_limited_count = min(1000, state.recent_rate_limited_count + 1)

            is_primary_exhausted = (
                state.remaining == 0 and state.primary_exhausted_until > now
            )
            if is_primary_exhausted:
                return

            # Secondary-like limit (403 with remaining quota): apply per-bucket cooldown.
            base_seconds = max(1, settings.github_secondary_limit_base_backoff_sec)
            max_seconds = max(base_seconds, settings.github_max_backoff_sec)
            exponential_seconds = min(
                max_seconds,
                base_seconds * (2 ** min(retry_count, 6)),
            )
            state.secondary_cooldown_until = max(
                state.secondary_cooldown_until,
                now + exponential_seconds + jitter_seconds,
            )

    def mark_success(self, bucket: str) -> None:
        now = time.time()
        with self._lock:
            state = self._states[bucket]
            state.recent_rate_limited_count = max(0, state.recent_rate_limited_count - 1)
            if state.secondary_cooldown_until <= now:
                state.secondary_cooldown_until = 0.0
            if state.retry_after_until <= now:
                state.retry_after_until = 0.0


class GitHubApiAdapter:
    BASE_URL = "https://api.github.com"
    RATE_LIMIT_STATUS_CODES = {403, 429}
    RETRYABLE_STATUS_CODES = {500, 502, 503, 504}
    MAX_RETRY_COUNT = 3
    _shared_rate_limit_state = _SharedRateLimitState()

    def __init__(self, concurrency: int = 1) -> None:
        self.global_concurrency = max(1, concurrency)
        self.search_concurrency = max(
            1,
            min(self.global_concurrency, settings.seed_github_search_concurrency),
        )
        self.core_concurrency = max(
            1,
            min(self.global_concurrency, settings.seed_github_core_concurrency),
        )
        self.headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        }
        if settings.github_token:
            self.headers["Authorization"] = f"Bearer {settings.github_token}"

        self.proxy = "socks5://tor-proxy:9050"
        self._client: httpx.AsyncClient | None = None
        self._global_semaphore = asyncio.Semaphore(self.global_concurrency)
        self._bucket_semaphores = {
            "search": asyncio.Semaphore(self.search_concurrency),
            "core": asyncio.Semaphore(self.core_concurrency),
        }

    def _build_async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers=self.headers,
            proxy=self.proxy,
            timeout=httpx.Timeout(settings.request_timeout_seconds),
            limits=httpx.Limits(
                max_connections=self.global_concurrency,
                max_keepalive_connections=self.global_concurrency,
            ),
            trust_env=False,
        )

    async def __aenter__(self) -> "GitHubApiAdapter":
        self._client = self._build_async_client()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _classify_bucket(self, path_or_url: str) -> str:
        parsed = urlparse(path_or_url)
        path = parsed.path if parsed.scheme else path_or_url
        if path.startswith("/search/"):
            return "search"
        return "core"

    async def _wait_for_bucket_availability(self, bucket: str) -> None:
        while True:
            delay_seconds = self._shared_rate_limit_state.next_delay_seconds(bucket)
            if delay_seconds <= 0:
                return

            sleep_seconds = min(delay_seconds, 30.0)
            logger.info(
                "Rate limit admission hold: bucket=%s sleep=%.2fs",
                bucket,
                sleep_seconds,
            )
            await asyncio.sleep(sleep_seconds)

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
    ) -> httpx.Response:
        if self._client is None:
            raise RuntimeError("GitHubApiAdapter client is not initialized")

        url = path if path.startswith("http://") or path.startswith("https://") else f"{self.BASE_URL}{path}"
        bucket = self._classify_bucket(url)

        for retry_count in range(self.MAX_RETRY_COUNT + 1):
            await self._wait_for_bucket_availability(bucket)
            await asyncio.sleep(random.uniform(0.05, 0.20))

            try:
                async with self._global_semaphore:
                    async with self._bucket_semaphores[bucket]:
                        response = await self._client.request(method, url, params=params)
            except httpx.RequestError as exc:
                if retry_count < self.MAX_RETRY_COUNT:
                    await asyncio.sleep(min(2 ** retry_count, settings.github_max_backoff_sec))
                    continue
                raise exc

            # Response accounting happens for all statuses, not only error statuses.
            self._shared_rate_limit_state.apply_headers(bucket, response.headers)

            if response.status_code in self.RATE_LIMIT_STATUS_CODES:
                self._shared_rate_limit_state.mark_rate_limited(
                    bucket,
                    response.headers,
                    retry_count=retry_count,
                )
                if retry_count < self.MAX_RETRY_COUNT:
                    continue
                logger.warning(
                    "Rate-limited after retries: bucket=%s status=%s path=%s",
                    bucket,
                    response.status_code,
                    path,
                )

            if (
                response.status_code in self.RETRYABLE_STATUS_CODES
                and retry_count < self.MAX_RETRY_COUNT
            ):
                await asyncio.sleep(min(2 ** retry_count, settings.github_max_backoff_sec))
                continue

            self._shared_rate_limit_state.mark_success(bucket)
            return response

        raise RuntimeError("unreachable")

    async def get_json(
        self,
        path: str,
        *,
        params: dict | None = None,
        not_found_none: bool = False,
    ) -> dict | list | None:
        response = await self.request("GET", path, params=params)
        if response.status_code == 404 and not_found_none:
            return None

        response.raise_for_status()
        return response.json()

    async def get_paginated_items(
        self,
        path: str,
        *,
        params: dict | None = None,
        item_key: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        collected_items: list[dict] = []
        page = 1

        while limit is None or len(collected_items) < limit:
            remaining = 100 if limit is None else max(0, limit - len(collected_items))
            if remaining == 0:
                break

            request_params = dict(params or {})
            request_params["per_page"] = min(100, remaining)
            request_params["page"] = page

            payload = await self.get_json(path, params=request_params)
            if item_key:
                page_items = (payload or {}).get(item_key) or []
            else:
                page_items = payload or []

            if not isinstance(page_items, list) or not page_items:
                break

            for item in page_items:
                if isinstance(item, dict):
                    collected_items.append(item)
                    if limit is not None and len(collected_items) >= limit:
                        break

            if len(page_items) < request_params["per_page"]:
                break

            page += 1

        return collected_items
