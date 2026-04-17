from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_pubdev


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class PubDevAdapter:
    BASE_URL = "https://pub.dev/api/packages"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        latest = payload.get("latest") or {}
        pubspec = latest.get("pubspec") or {}

        compact_payload = {
            "registry_name": "pubdev",
            "source_reference_url": f"{self.BASE_URL}/{package_name}",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": payload.get("name"),
            "latest": {
                "version": latest.get("version"),
                "pubspec": {
                    "name": pubspec.get("name"),
                    "description": pubspec.get("description"),
                    "homepage": pubspec.get("homepage"),
                    "repository": pubspec.get("repository"),
                    "issue_tracker": pubspec.get("issue_tracker"),
                    "documentation": pubspec.get("documentation"),
                },
            },
        }

        return RawPackageMetadata(
            registry_name="pubdev",
            package_name=package_name,
            package_version=latest.get("version"),
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_pubdev(compact_payload),
            full_raw_metadata=payload,
        )
