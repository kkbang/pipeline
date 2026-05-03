from dataclasses import dataclass
from datetime import datetime, timezone

from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_swiftpm


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class SwiftPMAdapter:
    SEARCH_BASE_URL = "https://swiftpackageindex.com/search?query="

    @staticmethod
    def _resolve_repository_url(package_name: str) -> str | None:
        normalized = package_name.strip()
        if not normalized:
            return None

        if normalized.startswith("http://") or normalized.startswith("https://"):
            return normalized

        if "/" in normalized:
            owner, repo = normalized.split("/", 1)
            owner = owner.strip()
            repo = repo.strip()
            if owner and repo:
                return f"https://github.com/{owner}/{repo}"

        return None

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        repository_url = self._resolve_repository_url(package_name)
        compact_payload = {
            "registry_name": "swiftpm",
            "source_reference_url": f"{self.SEARCH_BASE_URL}{package_name}",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": package_name,
            "repository_url": repository_url,
        }

        return RawPackageMetadata(
            registry_name="swiftpm",
            package_name=package_name,
            package_version=None,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_swiftpm(compact_payload),
            full_raw_metadata=None,
        )
