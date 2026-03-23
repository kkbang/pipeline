import requests
from datetime import datetime, timezone
from dataclasses import dataclass

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_pypi


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class PyPIAdapter:
    BASE_URL = "https://pypi.org/pypi"

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        response = requests.get(
            f"{self.BASE_URL}/{package_name}/json",
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        info = payload.get("info", {})
        compact_payload = {
            "registry_name": "pypi",
            "source_reference_url": f"{self.BASE_URL}/{package_name}/json",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "info": {
                "name": info.get("name"),
                "version": info.get("version"),
                "summary": info.get("summary"),
                "license": info.get("license"),
                "home_page": info.get("home_page"),
                "project_urls": info.get("project_urls") or {},
            },
        }

        return RawPackageMetadata(
            registry_name="pypi",
            package_name=package_name,
            package_version=info.get("version"),
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_pypi(compact_payload),
            full_raw_metadata=payload,
        )
