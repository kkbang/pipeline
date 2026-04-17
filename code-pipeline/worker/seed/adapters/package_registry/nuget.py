from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_nuget


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class NuGetAdapter:
    BASE_URL = "https://api.nuget.org/v3/registration5-semver1"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        normalized_name = package_name.strip().lower()
        response = requests.get(
            f"{self.BASE_URL}/{normalized_name}/index.json",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        catalog_entries = []
        for page in payload.get("items") or []:
            page_items = page.get("items")
            if not isinstance(page_items, list):
                page_url = page.get("@id")
                if isinstance(page_url, str) and page_url.strip():
                    page_response = requests.get(
                        page_url.strip(),
                        timeout=settings.request_timeout_seconds,
                    )
                    page_response.raise_for_status()
                    page_items = (page_response.json().get("items") or [])
                else:
                    page_items = []

            for item in page_items:
                catalog_entry = item.get("catalogEntry") or {}
                if isinstance(catalog_entry, dict) and catalog_entry:
                    catalog_entries.append(catalog_entry)

        latest_catalog_entry = catalog_entries[-1] if catalog_entries else {}
        package_version = latest_catalog_entry.get("version")
        compact_payload = {
            "registry_name": "nuget",
            "source_reference_url": f"{self.BASE_URL}/{normalized_name}/index.json",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "name": latest_catalog_entry.get("id") or package_name,
            "latest_catalog_entry": {
                "version": latest_catalog_entry.get("version"),
                "projectUrl": latest_catalog_entry.get("projectUrl"),
                "repositoryUrl": latest_catalog_entry.get("repositoryUrl"),
                "licenseUrl": latest_catalog_entry.get("licenseUrl"),
                "iconUrl": latest_catalog_entry.get("iconUrl"),
            },
        }

        return RawPackageMetadata(
            registry_name="nuget",
            package_name=package_name,
            package_version=package_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_nuget(compact_payload),
            full_raw_metadata=payload,
        )
