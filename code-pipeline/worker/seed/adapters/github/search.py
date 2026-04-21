from dataclasses import dataclass
from datetime import datetime, timezone

from worker.common.config import settings
from .api import GitHubApiAdapter


@dataclass
class RawGitHubSearchRepoMetadata:
    source_name: str
    source_item_id: str
    query: str
    repo_name: str
    raw_metadata: dict
    candidate_repo_urls: list[str]


class GitHubSearchAdapter:
    def __init__(self, api_adapter: GitHubApiAdapter) -> None:
        self.api_adapter = api_adapter

    async def search_repositories(
        self,
        list_name: str,
        search_query: dict,
    ) -> list[RawGitHubSearchRepoMetadata]:
        query = str(search_query.get("query") or "").strip()
        if not query:
            return []

        # Limit is controlled only by env (`GITHUB_SEARCH_REPO_LIMIT`).
        # GitHub Search API returns up to 1000 results per query.
        repo_limit = max(1, min(settings.github_search_repo_limit, 1000))
        sort = str(search_query.get("sort") or "stars").strip() or "stars"
        order = str(search_query.get("order") or "desc").strip() or "desc"

        repo_metadatas: list[RawGitHubSearchRepoMetadata] = []
        items = await self.api_adapter.get_paginated_items(
            "/search/repositories",
            params={
                "q": query,
                "sort": sort,
                "order": order,
            },
            item_key="items",
            limit=repo_limit,
        )

        for item in items:
            repo_name = item.get("name")
            full_name = item.get("full_name") or ""
            repo_url = item.get("html_url")

            compact_payload = {
                "source_type": "github_search_repo",
                "source_name": list_name,
                "source_item_id": full_name,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "query": query,
                "full_name": full_name,
                "repo_name": repo_name,
                "repo_url": repo_url,
                "description": item.get("description"),
                "language": item.get("language"),
                "license": ((item.get("license") or {}).get("spdx_id")),
                "is_fork": item.get("fork", False),
                "is_archived": item.get("archived", False),
                "stars": item.get("stargazers_count"),
                "forks_count": item.get("forks_count"),
                "source_reference_url": item.get("url"),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "pushed_at": item.get("pushed_at"),
            }

            candidate_repo_urls = []
            if isinstance(repo_url, str) and repo_url.strip():
                candidate_repo_urls.append(repo_url.strip())

            repo_metadatas.append(
                RawGitHubSearchRepoMetadata(
                    source_name=list_name,
                    source_item_id=full_name,
                    query=query,
                    repo_name=repo_name or full_name,
                    raw_metadata=compact_payload,
                    candidate_repo_urls=candidate_repo_urls,
                )
            )

        return repo_metadatas
