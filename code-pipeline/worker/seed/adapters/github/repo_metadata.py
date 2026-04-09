import asyncio
from dataclasses import dataclass

from worker.common.config import settings
from .api import GitHubApiAdapter


@dataclass(slots=True)
class RepoMetadataFetchResult:
    # 각 repo fetch 결과를 담는 객체
    owner: str
    repo: str
    metadata: dict | None = None
    error: Exception | None = None


class GitHubRepoMetadataAdapter:
    def fetch_repo_metadata(self, owner: str, repo: str) -> dict | None:
        return asyncio.run(self.fetch_repo_metadata_async(owner, repo))

    async def fetch_repo_metadata_async(self, owner: str, repo: str) -> dict | None:
        async with GitHubApiAdapter(concurrency=1) as adapter:
            return await adapter.get_json(
                f"/repos/{owner}/{repo}",
                not_found_none=True,
            )

    async def fetch_repo_metadata_batch(
        self,
        repo_keys: list[tuple[str, str]],
        concurrency: int | None = None,
    ) -> dict[tuple[str, str], RepoMetadataFetchResult]:
        limit = max(1, concurrency or settings.github_repo_metadata_concurrency)
        semaphore = asyncio.Semaphore(limit)

        async with GitHubApiAdapter(concurrency=limit) as adapter:
            async def _fetch(owner: str, repo: str) -> RepoMetadataFetchResult:
                async with semaphore:
                    try:
                        metadata = await adapter.get_json(
                            f"/repos/{owner}/{repo}",
                            not_found_none=True,
                        )
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
