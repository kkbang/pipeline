import asyncio
import random
import socket
import threading
import time
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from stem import Signal
from stem.control import Controller
from urllib3.util.retry import Retry

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
        self.proxies = {
            "http": "socks5h://tor-proxy:9050",
            "https": "socks5h://tor-proxy:9050",
        }

        # 여러 스레드가 동시에 Tor IP를 바꾸지 않도록 lock 사용
        self._tor_lock = threading.Lock()

    def _build_session(self) -> requests.Session:
        # requests.Session은 요청마다 새로 만드는 편이 병렬 처리에서 안전함
        retry_strategy = Retry(
            total=3,
            status_forcelist=[500, 502, 503, 504],
            backoff_factor=1,
        )
        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        return session

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

    def _fetch_repo_metadata_with_session(
        self,
        session: requests.Session,
        owner: str,
        repo: str,
        retry_count: int = 0,
    ) -> dict | None:
        # 요청이 한꺼번에 몰리지 않도록 약간 랜덤 지연
        time.sleep(random.uniform(1.0, 3.0))

        try:
            url = f"{self.BASE_URL}/repos/{owner}/{repo}"

            response = session.get(
                url,
                headers=self.headers,
                proxies=self.proxies,
                timeout=settings.request_timeout_seconds,
            )

            # rate limit이 걸리면 Tor IP 교체 후 재시도
            if response.status_code in [403, 429]:
                if retry_count < 3:
                    print(f"[403/429] Limit 도달! IP 교체 후 다시 시도합니다. (시도 {retry_count + 1}/3)")
                    self.renew_tor_ip()
                    return self._fetch_repo_metadata_with_session(
                        session,
                        owner,
                        repo,
                        retry_count + 1,
                    )

                print("IP 교체를 3번 시도했으나 계속 실패했습니다.")
                response.raise_for_status()

            # repo가 없으면 None 반환
            if response.status_code == 404:
                return None

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error fetching metadata for {owner}/{repo}: {e}")
            raise

    def fetch_repo_metadata(self, owner: str, repo: str, retry_count: int = 0) -> dict | None:
        # 기존 동기 함수
        with self._build_session() as session:
            return self._fetch_repo_metadata_with_session(session, owner, repo, retry_count)

    async def fetch_repo_metadata_async(self, owner: str, repo: str) -> dict | None:
        # requests는 blocking이므로 thread로 넘겨서 async처럼 사용
        return await asyncio.to_thread(self.fetch_repo_metadata, owner, repo)

    async def fetch_repo_metadata_batch(
        self,
        repo_keys: list[tuple[str, str]],
        concurrency: int | None = None,
    ) -> dict[tuple[str, str], RepoMetadataFetchResult]:
        limit = max(1, concurrency or settings.github_repo_resolver_concurrency)
        semaphore = asyncio.Semaphore(limit)

        async def _fetch(owner: str, repo: str) -> RepoMetadataFetchResult:
            async with semaphore:
                try:
                    metadata = await self.fetch_repo_metadata_async(owner, repo)
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
