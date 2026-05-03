import asyncio
import hashlib
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


@dataclass(slots=True)
class _TokenCredential:
    key: str
    token: str | None = None


class _SharedRateLimitState:
    def __init__(self) -> None:
        self._states: dict[str, _BucketRateLimitState] = defaultdict(_BucketRateLimitState)
        self._lock = threading.Lock()

    def next_delay_seconds(self, state_key: str) -> float:
        now = time.time()
        with self._lock:
            state = self._states[state_key]
            blocked_until = max(
                state.retry_after_until,
                state.primary_exhausted_until,
                state.secondary_cooldown_until,
            )
            return max(0.0, blocked_until - now)

    def apply_headers(self, state_key: str, headers: httpx.Headers) -> None:
        now = time.time()
        remaining = _parse_int(headers.get("X-RateLimit-Remaining"))
        reset_at = _parse_int(headers.get("X-RateLimit-Reset"))
        retry_after_seconds = _parse_retry_after_seconds(headers.get("Retry-After"))

        with self._lock:
            state = self._states[state_key]
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

    def mark_rate_limited(self, state_key: str, headers: httpx.Headers, retry_count: int) -> None:
        self.apply_headers(state_key, headers)
        now = time.time()
        jitter_seconds = random.uniform(
            0.0,
            max(0.0, settings.github_bucket_cooldown_jitter_sec),
        )

        with self._lock:
            state = self._states[state_key]
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

    def mark_success(self, state_key: str) -> None:
        now = time.time()
        with self._lock:
            state = self._states[state_key]
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
    _tor_rotation_lock = threading.Lock()
    _tor_last_rotation_epoch = 0.0

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
        self._base_headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        }
        self._token_credentials = self._build_token_credentials()
        self._token_rr_counter: dict[str, int] = defaultdict(int)
        self._last_selected_token_key_by_bucket: dict[str, str] = {}

        self.proxy = "socks5://tor-proxy:9050"
        self._client: httpx.AsyncClient | None = None
        self._global_semaphore = asyncio.Semaphore(self.global_concurrency)
        self._bucket_semaphores = {
            "search": asyncio.Semaphore(self.search_concurrency),
            "core": asyncio.Semaphore(self.core_concurrency),
        }

    def _build_async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers=self._base_headers,
            proxy=self.proxy,
            timeout=httpx.Timeout(settings.request_timeout_seconds),
            limits=httpx.Limits(
                max_connections=self.global_concurrency,
                max_keepalive_connections=self.global_concurrency,
            ),
            trust_env=False,
        )

    def _build_token_credentials(self) -> list[_TokenCredential]:
        raw_tokens = []
        for token in settings.github_tokens:
            if isinstance(token, str) and token.strip():
                raw_tokens.append(token.strip())

        if not raw_tokens and settings.github_token.strip():
            raw_tokens.append(settings.github_token.strip())

        if not raw_tokens:
            return [_TokenCredential(key="anon", token=None)]

        credentials: list[_TokenCredential] = []
        for index, token in enumerate(raw_tokens, start=1):
            token_digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:8]
            credentials.append(
                _TokenCredential(
                    key=f"token_{index}_{token_digest}",
                    token=token,
                )
            )
        return credentials

    def _build_request_headers(self, credential: _TokenCredential) -> dict[str, str]:
        headers = dict(self._base_headers)
        if credential.token:
            headers["Authorization"] = f"Bearer {credential.token}"
        return headers

    def _credential_state_key(self, credential: _TokenCredential, bucket: str) -> str:
        return f"{credential.key}:{bucket}"

    def _select_token_for_bucket(self, bucket: str) -> tuple[_TokenCredential, float]:
        candidates: list[_TokenCredential] = []
        best_delay: float | None = None

        for credential in self._token_credentials:
            state_key = self._credential_state_key(credential, bucket)
            delay_seconds = self._shared_rate_limit_state.next_delay_seconds(state_key)

            if best_delay is None or delay_seconds < (best_delay - 1e-9):
                best_delay = delay_seconds
                candidates = [credential]
            elif abs(delay_seconds - best_delay) <= 1e-9:
                candidates.append(credential)

        if best_delay is None or not candidates:
            return self._token_credentials[0], 0.0

        rr_index = self._token_rr_counter[bucket] % len(candidates)
        self._token_rr_counter[bucket] += 1
        return candidates[rr_index], best_delay

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

    def _is_primary_limit_response(self, headers: httpx.Headers) -> bool:
        now = time.time()
        remaining = _parse_int(headers.get("X-RateLimit-Remaining"))
        reset_at = _parse_int(headers.get("X-RateLimit-Reset"))
        return remaining == 0 and reset_at is not None and float(reset_at) > now

    def _rotate_tor_ip(self, *, reason: str) -> bool:
        if settings.github_tor_rotation_enabled is not True:
            return False

        now = time.time()
        min_interval_seconds = max(0, settings.github_tor_rotation_min_interval_sec)
        wait_seconds = max(0.0, settings.github_tor_newnym_wait_sec)

        with self._tor_rotation_lock:
            since_last = now - self.__class__._tor_last_rotation_epoch
            if since_last < min_interval_seconds:
                logger.info(
                    (
                        "Tor rotation skipped by cooldown: reason=%s "
                        "since_last=%.2fs min_interval=%ss"
                    ),
                    reason,
                    since_last,
                    min_interval_seconds,
                )
                return False

            try:
                import socket
                from stem import Signal
                from stem.control import Controller
            except Exception as exc:
                logger.warning("Tor rotation unavailable: reason=%s error=%s", reason, exc)
                return False

            host = settings.github_tor_control_host.strip() or "tor-proxy"
            port = max(1, int(settings.github_tor_control_port))
            password = settings.github_tor_control_password

            try:
                tor_control_ip = socket.gethostbyname(host)
                with Controller.from_port(address=tor_control_ip, port=port) as controller:
                    if password:
                        controller.authenticate(password=password)
                    else:
                        controller.authenticate()
                    controller.signal(Signal.NEWNYM)
                self.__class__._tor_last_rotation_epoch = now
                logger.info(
                    "Tor identity rotated: reason=%s host=%s port=%s wait=%.1fs",
                    reason,
                    host,
                    port,
                    wait_seconds,
                )
            except Exception as exc:
                logger.warning("Tor rotation failed: reason=%s error=%s", reason, exc)
                return False

        if wait_seconds > 0:
            time.sleep(wait_seconds)
        return True

    async def _rotate_tor_ip_async(self, *, reason: str) -> bool:
        return await asyncio.to_thread(self._rotate_tor_ip, reason=reason)

    async def _wait_for_bucket_availability(self, bucket: str) -> _TokenCredential:
        while True:
            credential, delay_seconds = self._select_token_for_bucket(bucket)
            previous_key = self._last_selected_token_key_by_bucket.get(bucket)
            if previous_key != credential.key:
                logger.info(
                    (
                        "GitHub token switched: bucket=%s previous_token=%s "
                        "selected_token=%s selected_delay=%.2fs"
                    ),
                    bucket,
                    previous_key,
                    credential.key,
                    delay_seconds,
                )
                self._last_selected_token_key_by_bucket[bucket] = credential.key
            if delay_seconds <= 0:
                return credential

            sleep_seconds = min(delay_seconds, 30.0)
            logger.info(
                "Rate limit admission hold: bucket=%s token=%s sleep=%.2fs",
                bucket,
                credential.key,
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
            credential = await self._wait_for_bucket_availability(bucket)
            state_key = self._credential_state_key(credential, bucket)
            request_headers = self._build_request_headers(credential)
            await asyncio.sleep(random.uniform(0.05, 0.20))

            try:
                async with self._global_semaphore:
                    async with self._bucket_semaphores[bucket]:
                        response = await self._client.request(
                            method,
                            url,
                            params=params,
                            headers=request_headers,
                        )
            except httpx.RequestError as exc:
                if retry_count < self.MAX_RETRY_COUNT:
                    await asyncio.sleep(min(2 ** retry_count, settings.github_max_backoff_sec))
                    continue
                raise exc

            # Response accounting happens for all statuses, not only error statuses.
            self._shared_rate_limit_state.apply_headers(state_key, response.headers)

            if response.status_code in self.RATE_LIMIT_STATUS_CODES:
                self._shared_rate_limit_state.mark_rate_limited(
                    state_key,
                    response.headers,
                    retry_count=retry_count,
                )
                # If only a single usable credential exists, allow Tor rotation for
                # core endpoints too; otherwise repeated 403s can stall on one token.
                allow_rotation_for_bucket = (
                    settings.github_tor_rotate_only_on_search is not True
                    or bucket == "search"
                    or len(self._token_credentials) <= 1
                )
                is_primary_limited = self._is_primary_limit_response(response.headers)
                should_try_rotation = (
                    settings.github_tor_rotation_enabled is True
                    and allow_rotation_for_bucket
                    and not is_primary_limited
                )
                if should_try_rotation:
                    logger.info(
                        (
                            "Tor rotation decision: action=attempt bucket=%s token=%s "
                            "status=%s remaining=%s retry_after=%s path=%s"
                        ),
                        bucket,
                        credential.key,
                        response.status_code,
                        response.headers.get("X-RateLimit-Remaining"),
                        response.headers.get("Retry-After"),
                        path,
                    )
                    await self._rotate_tor_ip_async(
                        reason=f"rate_limit:{bucket}:{response.status_code}",
                    )
                else:
                    if settings.github_tor_rotation_enabled is not True:
                        skip_reason = "rotation_disabled"
                    elif not allow_rotation_for_bucket:
                        skip_reason = "bucket_not_allowed"
                    elif is_primary_limited:
                        skip_reason = "primary_limit_exhausted"
                    else:
                        skip_reason = "unknown"
                    logger.info(
                        (
                            "Tor rotation decision: action=skip reason=%s bucket=%s token=%s "
                            "status=%s remaining=%s retry_after=%s path=%s"
                        ),
                        skip_reason,
                        bucket,
                        credential.key,
                        response.status_code,
                        response.headers.get("X-RateLimit-Remaining"),
                        response.headers.get("Retry-After"),
                        path,
                    )
                if retry_count < self.MAX_RETRY_COUNT:
                    continue
                logger.warning(
                    "Rate-limited after retries: bucket=%s token=%s status=%s path=%s",
                    bucket,
                    credential.key,
                    response.status_code,
                    path,
                )

            if (
                response.status_code in self.RETRYABLE_STATUS_CODES
                and retry_count < self.MAX_RETRY_COUNT
            ):
                await asyncio.sleep(min(2 ** retry_count, settings.github_max_backoff_sec))
                continue

            self._shared_rate_limit_state.mark_success(state_key)
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
