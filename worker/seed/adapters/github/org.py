from dataclasses import dataclass
from datetime import datetime, timezone

from worker.common.config import settings
from .api import GitHubApiAdapter


@dataclass
class RawGitHubOrgRepoMetadata:
    source_name: str
    source_item_id: str
    org_name: str
    repo_name: str
    raw_metadata: dict
    candidate_repo_urls: list[str]


class GitHubOrgAdapter:
    def __init__(self, api_adapter: GitHubApiAdapter) -> None:
        self.api_adapter = api_adapter

    async def fetch_org_repositories(
        self,
        list_name: str,
        org_name: str,
        max_repos: int | None = None,
    ) -> list[RawGitHubOrgRepoMetadata]:
        repo_limit = max_repos or settings.github_org_repo_limit
        repo_metadatas: list[RawGitHubOrgRepoMetadata] = []

        payload = await self.api_adapter.get_paginated_items(
            f"/orgs/{org_name}/repos",
            params={
                "type": "public",
                "sort": "updated",
            },
            limit=repo_limit,
        )

        for item in payload:
            repo_name = item.get("name")
            owner_login = ((item.get("owner") or {}).get("login")) or org_name
            full_name = item.get("full_name") or f"{owner_login}/{repo_name}"
            repo_url = item.get("html_url")

            compact_payload = {
                "source_type": "org_repo",
                "source_name": list_name,
                "source_item_id": full_name,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "org_name": org_name,
                "repo_name": repo_name,
                "full_name": full_name,
                "repo_url": repo_url,
                "description": item.get("description"),
                "language": item.get("language"),
                "license": ((item.get("license") or {}).get("spdx_id")),
                "is_fork": item.get("fork", False),
                "is_archived": item.get("archived", False),
                "stars": item.get("stargazers_count"),
                "forks_count": item.get("forks_count"),
                "source_reference_url": f"https://api.github.com/repos/{full_name}",
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "pushed_at": item.get("pushed_at"),
            }

            candidate_repo_urls = []
            if isinstance(repo_url, str) and repo_url.strip():
                candidate_repo_urls.append(repo_url.strip())

            repo_metadatas.append(
                RawGitHubOrgRepoMetadata(
                    source_name=list_name,
                    source_item_id=full_name,
                    org_name=org_name,
                    repo_name=repo_name,
                    raw_metadata=compact_payload,
                    candidate_repo_urls=candidate_repo_urls,
                )
            )

        return repo_metadatas
