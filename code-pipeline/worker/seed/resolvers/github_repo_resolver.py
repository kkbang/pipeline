import asyncio
import httpx
import random
import socket
import threading
import time
from dataclasses import dataclass

from stem import Signal
from stem.control import Controller

from worker.common.config import settings


@dataclass(slots=True)
class RepoMetadataFetchResult:
    # 각 repo fetch 결과를 담는 객체
    owner: str
    repo: str
    metadata: dict | None = None
    error: Exception | None = None


class GitHubRepoResolver:
    BASE_URL = "https://api.github.com"
    RATE_LIMIT_STATUS_CODES = {403, 429}
    RETRYABLE_STATUS_CODES = {500, 502, 503, 504}
    MAX_RETRY_COUNT = 3

    def __init__(self) -> None:
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

        # 여러 스레드가 동시에 Tor IP를 바꾸지 않도록 lock 사용
        self._tor_lock = threading.Lock()

    def _build_async_client(self, concurrency: int) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers=self.headers,
            proxy=self.proxy,
            timeout=httpx.Timeout(settings.request_timeout_seconds),
            limits=httpx.Limits(
                max_connections=max(concurrency, 1),
                max_keepalive_connections=max(concurrency, 1),
            ),
            trust_env=False,
        )

    def renew_tor_ip(self) -> None:
        try:
            with self._tor_lock:
                # stem은 control host에 IP 주소를 기대하므로 service hostname을 먼저 해석
                tor_control_ip = socket.gethostbyname("tor-proxy")
                with Controller.from_port(address=tor_control_ip, port=9051) as controller:
                    controller.authenticate()
                    controller.signal(Signal.NEWNYM)
                    print("[Tor] IP 교체 신호를 보냈습니다. 10초 대기...")
                    time.sleep(10)
        except Exception as e:
            print(f"[Tor] IP 교체 실패: {e}")

    async def renew_tor_ip_async(self) -> None:
        await asyncio.to_thread(self.renew_tor_ip)

    async def _fetch_repo_metadata_with_client(
        self,
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
    ) -> dict | None:
        url = f"{self.BASE_URL}/repos/{owner}/{repo}"

        for retry_count in range(self.MAX_RETRY_COUNT + 1):
            await asyncio.sleep(random.uniform(1.0, 3.0))

            try:
                response = await client.get(url)
            except httpx.RequestError as exc:
                if retry_count < self.MAX_RETRY_COUNT:
                    await asyncio.sleep(retry_count + 1)
                    continue
                print(f"Error fetching metadata for {owner}/{repo}: {exc}")
                raise

            if response.status_code == 404:
                return None

            if response.status_code in self.RATE_LIMIT_STATUS_CODES:
                if retry_count < self.MAX_RETRY_COUNT:
                    print(f"[403/429] Limit 도달! IP 교체 후 다시 시도합니다. (시도 {retry_count + 1}/3)")
                    await self.renew_tor_ip_async()
                    continue
                print("IP 교체를 3번 시도했으나 계속 실패했습니다.")
                response.raise_for_status()

            if response.status_code in self.RETRYABLE_STATUS_CODES and retry_count < self.MAX_RETRY_COUNT:
                await asyncio.sleep(retry_count + 1)
                continue

            response.raise_for_status()
            return response.json()

        return None

    def fetch_repo_metadata(self, owner: str, repo: str) -> dict | None:
        return asyncio.run(self.fetch_repo_metadata_async(owner, repo))

    async def fetch_repo_metadata_async(self, owner: str, repo: str) -> dict | None:
        async with self._build_async_client(concurrency=1) as client:
            return await self._fetch_repo_metadata_with_client(client, owner, repo)

    async def fetch_repo_metadata_batch(
        self,
        repo_keys: list[tuple[str, str]],
        concurrency: int | None = None,
    ) -> dict[tuple[str, str], RepoMetadataFetchResult]:
        limit = max(1, concurrency or settings.github_repo_resolver_concurrency)
        semaphore = asyncio.Semaphore(limit)

        async with self._build_async_client(concurrency=limit) as client:
            async def _fetch(owner: str, repo: str) -> RepoMetadataFetchResult:
                async with semaphore:
                    try:
                        metadata = await self._fetch_repo_metadata_with_client(client, owner, repo)
                        return RepoMetadataFetchResult(
                            owner=owner,
                            repo=repo,
                            metadata=metadata,
                        )
                    except Exception as exc:
                        return RepoMetadataFetchResult(
                            owner=owner,
                            repo=repo,
                            error=exc,
                        )

            # 여러 repo 요청을 동시에 실행
            results = await asyncio.gather(*(_fetch(owner, repo) for owner, repo in repo_keys))

        # 나중에 (owner, repo)로 바로 찾기 쉽도록 dict로 변환
        return {(result.owner, result.repo): result for result in results}
