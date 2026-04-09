from dataclasses import dataclass
from datetime import datetime, timezone

from worker.common.config import settings
from .api import GitHubApiAdapter


@dataclass
class RawGitHubTopicRepoMetadata:
    source_name: str
    source_item_id: str
    topic: str
    repo_name: str
    raw_metadata: dict
    candidate_repo_urls: list[str]


class GitHubTopicAdapter:
    def __init__(self, api_adapter: GitHubApiAdapter) -> None:
        self.api_adapter = api_adapter

    async def search_topic_repositories(
        self,
        list_name: str,
        topic_entry: dict,
    ) -> list[RawGitHubTopicRepoMetadata]:
        topic = str(topic_entry.get("topic") or "").strip()
        if not topic:
            return []

        extra_query = str(topic_entry.get("query") or "").strip()
        query = f"topic:{topic} archived:false"
        if extra_query:
            query = f"{query} {extra_query}"

        repo_limit = int(topic_entry.get("max_repos") or settings.github_topic_repo_limit)
        sort = str(topic_entry.get("sort") or "stars").strip() or "stars"
        order = str(topic_entry.get("order") or "desc").strip() or "desc"

        repo_metadatas: list[RawGitHubTopicRepoMetadata] = []
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
                "source_type": "github_topic_repo",
                "source_name": list_name,
                "source_item_id": full_name,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "topic": topic,
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
            }

            candidate_repo_urls = []
            if isinstance(repo_url, str) and repo_url.strip():
                candidate_repo_urls.append(repo_url.strip())

            repo_metadatas.append(
                RawGitHubTopicRepoMetadata(
                    source_name=list_name,
                    source_item_id=full_name,
                    topic=topic,
                    repo_name=repo_name or full_name,
                    raw_metadata=compact_payload,
                    candidate_repo_urls=candidate_repo_urls,
                )
            )

        return repo_metadatas
