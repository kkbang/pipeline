from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class RawCuratedRepoMetadata:
    source_name: str
    source_item_id: str
    raw_metadata: dict
    candidate_repo_urls: list[str]


class CuratedRepoListAdapter:
    def build_repo_metadata(self, list_name: str, repo_entry: str | dict) -> RawCuratedRepoMetadata:
        if isinstance(repo_entry, str):
            repo_url = repo_entry.strip()
            repo_label = None
            tags = []
            source_item_id = repo_url
        else:
            repo_url = (repo_entry.get("repo_url") or "").strip()
            repo_label = repo_entry.get("repo_label")
            tags = repo_entry.get("tags") or []
            source_item_id = str(repo_entry.get("source_item_id") or repo_url)

        if not repo_url:
            raise ValueError(f"Curated repo list entry in '{list_name}' is missing 'repo_url'")

        compact_payload = {
            "source_type": "curated_repo_list",
            "source_name": list_name,
            "source_item_id": source_item_id,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "repo_url": repo_url,
            "repo_label": repo_label,
            "tags": tags,
        }

        return RawCuratedRepoMetadata(
            source_name=list_name,
            source_item_id=source_item_id,
            raw_metadata=compact_payload,
            candidate_repo_urls=[repo_url],
        )
