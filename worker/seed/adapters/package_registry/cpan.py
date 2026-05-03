from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_cpan


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class CPANAdapter:
    BASE_URL = "https://fastapi.metacpan.org/v1/release"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        compact_payload = {
            "registry_name": "cpan",
            "source_reference_url": f"{self.BASE_URL}/{package_name}",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": payload.get("name"),
            "distribution": payload.get("distribution"),
            "version": payload.get("version"),
            "resources": payload.get("resources") or {},
            "download_url": payload.get("download_url"),
        }

        return RawPackageMetadata(
            registry_name="cpan",
            package_name=package_name,
            package_version=str(payload.get("version")) if payload.get("version") is not None else None,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_cpan(compact_payload),
            full_raw_metadata=payload,
        )
