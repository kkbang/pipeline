import re
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_hackage


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class HackageAdapter:
    BASE_URL = "https://hackage.haskell.org/package"

    @staticmethod
    def _extract_field(cabal_text: str, field_name: str) -> str | None:
        pattern = rf"(?im)^\s*{re.escape(field_name)}\s*:\s*(.+)$"
        match = re.search(pattern, cabal_text)
        if not match:
            return None
        value = match.group(1).strip()
        return value or None

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}/{package_name}.cabal",
            timeout=settings.request_timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()

        cabal_text = response.text
        final_url = response.url
        version_match = re.search(rf"/package/{re.escape(package_name)}-([^/]+)/", final_url)
        package_version = version_match.group(1) if version_match else None

        compact_payload = {
            "registry_name": "hackage",
            "source_reference_url": final_url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": package_name,
            "version": package_version,
            "cabal_fields": {
                "homepage": self._extract_field(cabal_text, "homepage"),
                "bug_reports": self._extract_field(cabal_text, "bug-reports"),
                "source_repository": self._extract_field(cabal_text, "location"),
            },
        }

        return RawPackageMetadata(
            registry_name="hackage",
            package_name=package_name,
            package_version=package_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_hackage(compact_payload),
            full_raw_metadata={
                "final_url": final_url,
                "cabal": cabal_text,
            },
        )
