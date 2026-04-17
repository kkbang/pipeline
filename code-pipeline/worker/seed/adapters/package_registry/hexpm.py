from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_hexpm


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class HexPMAdapter:
    BASE_URL = "https://hex.pm/api/packages"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        latest_version = payload.get("latest_stable_version")
        if not latest_version:
            releases = payload.get("releases") or []
            if releases and isinstance(releases[0], dict):
                latest_version = releases[0].get("version")

        compact_payload = {
            "registry_name": "hexpm",
            "source_reference_url": f"{self.BASE_URL}/{package_name}",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": payload.get("name"),
            "latest_stable_version": latest_version,
            "meta": payload.get("meta") or {},
            "html_url": payload.get("html_url"),
            "docs_html_url": payload.get("docs_html_url"),
        }

        return RawPackageMetadata(
            registry_name="hexpm",
            package_name=package_name,
            package_version=latest_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_hexpm(compact_payload),
            full_raw_metadata=payload,
        )
