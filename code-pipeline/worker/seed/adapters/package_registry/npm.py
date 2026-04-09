import requests
from datetime import datetime, timezone
from dataclasses import dataclass

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_npm


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class NPMAdapter:
    BASE_URL = "https://registry.npmjs.org"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        latest_version = ((payload.get("dist-tags") or {}).get("latest"))
        latest_metadata = ((payload.get("versions") or {}).get(latest_version) or {})
        compact_payload = {
            "registry_name": "npm",
            "source_reference_url": f"{self.BASE_URL}/{package_name}",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": payload.get("name"),
            "dist-tags": {"latest": latest_version},
            "repository": payload.get("repository"),
            "homepage": payload.get("homepage"),
            "bugs": payload.get("bugs"),
            "versions": {},
        }

        if latest_version:
            compact_payload["versions"][latest_version] = {
                "name": latest_metadata.get("name"),
                "version": latest_metadata.get("version"),
                "license": latest_metadata.get("license"),
                "repository": latest_metadata.get("repository"),
                "homepage": latest_metadata.get("homepage"),
                "bugs": latest_metadata.get("bugs"),
            }

        return RawPackageMetadata(
            registry_name="npm",
            package_name=package_name,
            package_version=latest_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_npm(compact_payload),
            full_raw_metadata=payload,
        )
