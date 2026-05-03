from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_cratesio


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class CratesIOAdapter:
    BASE_URL = "https://crates.io/api/v1/crates"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        crate = payload.get("crate") or {}
        package_version = crate.get("newest_version") or crate.get("max_stable_version")

        compact_payload = {
            "registry_name": "cratesio",
            "source_reference_url": f"{self.BASE_URL}/{package_name}",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "crate": {
                "name": crate.get("name"),
                "newest_version": crate.get("newest_version"),
                "max_stable_version": crate.get("max_stable_version"),
                "description": crate.get("description"),
                "repository": crate.get("repository"),
                "homepage": crate.get("homepage"),
                "documentation": crate.get("documentation"),
            },
        }

        return RawPackageMetadata(
            registry_name="cratesio",
            package_name=package_name,
            package_version=package_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_cratesio(compact_payload),
            full_raw_metadata=payload,
        )
