from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_packagist


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class PackagistAdapter:
    BASE_URL = "https://repo.packagist.org/p2"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}.json",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        versions = []
        packages = payload.get("packages") or {}
        for values in packages.values():
            if isinstance(values, list):
                versions = values
                break

        latest = versions[0] if versions else {}
        compact_payload = {
            "registry_name": "packagist",
            "source_reference_url": f"{self.BASE_URL}/{package_name}.json",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": latest.get("name") or package_name,
            "version": latest.get("version"),
            "description": latest.get("description"),
            "homepage": latest.get("homepage"),
            "source": latest.get("source") or {},
            "support": latest.get("support") or {},
            "packages": packages,
        }

        return RawPackageMetadata(
            registry_name="packagist",
            package_name=package_name,
            package_version=latest.get("version"),
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_packagist(compact_payload),
            full_raw_metadata=payload,
        )
