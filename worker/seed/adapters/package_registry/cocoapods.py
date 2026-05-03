import re
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_cocoapods


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class CocoaPodsAdapter:
    TRUNK_API_BASE_URL = "https://trunk.cocoapods.org/api/v1/pods"
    PAGE_BASE_URL = "https://cocoapods.org/pods"

    @staticmethod
    def _extract_urls_from_html(html: str) -> list[str]:
        return sorted(
            set(
                re.findall(
                    r"https?://(?:github\.com|gitlab\.com|bitbucket\.org|codeberg\.org)[^\s\"'<>]+",
                    html,
                )
            )
        )

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        latest_spec = {}
        trunk_payload = None

        trunk_response = requests.get(
            f"{self.TRUNK_API_BASE_URL}/{package_name}",
            timeout=settings.request_timeout_seconds,
        )
        if trunk_response.ok:
            trunk_payload = trunk_response.json()
            versions = trunk_payload.get("versions") or []
            if isinstance(versions, list) and versions:
                latest_spec = versions[-1] if isinstance(versions[-1], dict) else {}

        page_url = f"{self.PAGE_BASE_URL}/{package_name}"
        page_response = requests.get(
            page_url,
            timeout=settings.request_timeout_seconds,
        )
        page_response.raise_for_status()
        html_urls = self._extract_urls_from_html(page_response.text)

        package_version = None
        if latest_spec:
            package_version = (
                latest_spec.get("version")
                or latest_spec.get("name")
                or latest_spec.get("number")
            )

        compact_payload = {
            "registry_name": "cocoapods",
            "source_reference_url": page_url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": package_name,
            "latest_spec": latest_spec,
            "page_url": page_url,
            "json_url": f"{self.TRUNK_API_BASE_URL}/{package_name}",
        }

        candidate_repo_urls = extract_repo_candidates_from_cocoapods(compact_payload)
        for url in html_urls:
            if url not in candidate_repo_urls:
                candidate_repo_urls.append(url)

        return RawPackageMetadata(
            registry_name="cocoapods",
            package_name=package_name,
            package_version=package_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=candidate_repo_urls,
            full_raw_metadata={
                "trunk": trunk_payload,
                "page_html": page_response.text,
            },
        )
