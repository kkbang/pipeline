import asyncio
import logging
import random
import socket
import threading
import time

import httpx
from stem import Signal
from stem.control import Controller

from worker.common.config import settings


logger = logging.getLogger(__name__)


class GitHubApiAdapter:
    BASE_URL = "https://api.github.com"
    RATE_LIMIT_STATUS_CODES = {403, 429}
    RETRYABLE_STATUS_CODES = {500, 502, 503, 504}
    MAX_RETRY_COUNT = 3
    _tor_lock = threading.Lock()

    def __init__(self, concurrency: int = 1) -> None:
        self.concurrency = max(1, concurrency)
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

    def _build_async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers=self.headers,
            proxy=self.proxy,
            timeout=httpx.Timeout(settings.request_timeout_seconds),
            limits=httpx.Limits(
                max_connections=self.concurrency,
                max_keepalive_connections=self.concurrency,
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

    def renew_tor_ip(self) -> None:
        try:
            with self._tor_lock:
                tor_control_ip = socket.gethostbyname("tor-proxy")
                with Controller.from_port(address=tor_control_ip, port=9051) as controller:
                    controller.authenticate()
                    controller.signal(Signal.NEWNYM)
                    logger.info("[Tor] IP 교체 신호를 보냈습니다. 10초 대기...")
                    time.sleep(10)
        except Exception as exc:
            logger.info("[Tor] IP 교체 실패: %s", exc)

    async def renew_tor_ip_async(self) -> None:
        await asyncio.to_thread(self.renew_tor_ip)

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

        for retry_count in range(self.MAX_RETRY_COUNT + 1):
            await asyncio.sleep(random.uniform(0.1, 0.5))

            try:
                response = await self._client.request(method, url, params=params)
            except httpx.RequestError as exc:
                if retry_count < self.MAX_RETRY_COUNT:
                    await asyncio.sleep(retry_count + 1)
                    continue
                raise exc

            if response.status_code in self.RATE_LIMIT_STATUS_CODES:
                if retry_count < self.MAX_RETRY_COUNT:
                    logger.info(
                        "[403/429] Limit 도달! IP 교체 후 다시 시도합니다. (시도 %s/3)",
                        retry_count + 1,
                    )
                    await self.renew_tor_ip_async()
                    continue
                logger.info("IP 교체를 3번 시도했으나 계속 실패했습니다.")

            if (
                response.status_code in self.RETRYABLE_STATUS_CODES
                and retry_count < self.MAX_RETRY_COUNT
            ):
                await asyncio.sleep(retry_count + 1)
                continue

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
