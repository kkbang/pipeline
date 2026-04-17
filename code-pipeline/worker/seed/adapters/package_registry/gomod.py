from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_gomod


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class GoModuleAdapter:
    BASE_URL = "https://proxy.golang.org"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        module_path = package_name.strip()
        latest_payload = None
        latest_version = None

        if module_path:
            response = requests.get(
                f"{self.BASE_URL}/{module_path}/@latest",
                timeout=settings.request_timeout_seconds,
            )
            if response.ok:
                latest_payload = response.json()
                latest_version = latest_payload.get("Version")

        compact_payload = {
            "registry_name": "gomod",
            "source_reference_url": f"{self.BASE_URL}/{module_path}/@latest",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "module_path": module_path,
            "latest": latest_payload or {},
        }

        return RawPackageMetadata(
            registry_name="gomod",
            package_name=module_path,
            package_version=latest_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_gomod(compact_payload),
            full_raw_metadata=latest_payload,
        )
