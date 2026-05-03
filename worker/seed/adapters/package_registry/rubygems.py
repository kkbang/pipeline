from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_rubygems


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class RubyGemsAdapter:
    BASE_URL = "https://rubygems.org/api/v1/gems"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}.json",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        compact_payload = {
            "registry_name": "rubygems",
            "source_reference_url": f"{self.BASE_URL}/{package_name}.json",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": payload.get("name"),
            "version": payload.get("version"),
            "info": payload.get("info"),
            "licenses": payload.get("licenses") or [],
            "homepage_uri": payload.get("homepage_uri"),
            "source_code_uri": payload.get("source_code_uri"),
            "bug_tracker_uri": payload.get("bug_tracker_uri"),
            "documentation_uri": payload.get("documentation_uri"),
            "wiki_uri": payload.get("wiki_uri"),
        }

        return RawPackageMetadata(
            registry_name="rubygems",
            package_name=package_name,
            package_version=payload.get("version"),
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_rubygems(compact_payload),
            full_raw_metadata=payload,
        )
