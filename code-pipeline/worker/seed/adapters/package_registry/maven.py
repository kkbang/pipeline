from dataclasses import dataclass
from datetime import datetime, timezone
from xml.etree import ElementTree

import requests

from worker.common.config import settings
from worker.seed.extractors.repo_url_extractor import extract_repo_candidates_from_maven


@dataclass
class RawPackageMetadata:
    registry_name: str
    package_name: str
    package_version: str | None
    raw_metadata: dict
    candidate_repo_urls: list[str]
    full_raw_metadata: dict | None = None


class MavenCentralAdapter:
    SEARCH_URL = "https://search.maven.org/solrsearch/select"
    REPO_BASE_URL = "https://repo1.maven.org/maven2"

    @staticmethod
    def _parse_coordinates(package_name: str) -> tuple[str, str]:
        normalized = package_name.strip()
        parts = normalized.split(":")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                f"Invalid maven package '{package_name}'. Expected format is 'group:artifact'."
            )
        return parts[0].strip(), parts[1].strip()

    @staticmethod
    def _build_pom_url(group_id: str, artifact_id: str, version: str) -> str:
        group_path = group_id.replace(".", "/")
        return f"{MavenCentralAdapter.REPO_BASE_URL}/{group_path}/{artifact_id}/{version}/{artifact_id}-{version}.pom"

    @staticmethod
    def _strip_namespace(tag: str) -> str:
        if "}" in tag:
            return tag.split("}", 1)[1]
        return tag

    def _extract_pom_metadata(self, pom_text: str) -> dict:
        root = ElementTree.fromstring(pom_text)
        metadata = {
            "project_url": None,
            "scm_url": None,
            "scm_connection": None,
            "scm_developer_connection": None,
            "issue_management_url": None,
        }

        stack = [root]
        while stack:
            node = stack.pop()
            children = list(node)
            if children:
                stack.extend(reversed(children))

            tag = self._strip_namespace(node.tag)
            text = (node.text or "").strip() or None
            if not text:
                continue

            parent = node.getparent() if hasattr(node, "getparent") else None
            parent_tag = self._strip_namespace(parent.tag) if parent is not None else None

            if tag == "url" and parent_tag == "project":
                metadata["project_url"] = text
            elif tag == "url" and parent_tag == "scm":
                metadata["scm_url"] = text
            elif tag == "connection":
                metadata["scm_connection"] = text
            elif tag == "developerConnection":
                metadata["scm_developer_connection"] = text
            elif tag == "url" and parent_tag == "issueManagement":
                metadata["issue_management_url"] = text

        # ElementTree 기본 구현에서 getparent 미지원이므로 경로 탐색 fallback
        if metadata["project_url"] is None:
            project_url_node = root.find(".//{*}url")
            if project_url_node is not None and (project_url_node.text or "").strip():
                metadata["project_url"] = project_url_node.text.strip()

        scm = root.find(".//{*}scm")
        if scm is not None:
            scm_url = scm.find(".//{*}url")
            scm_connection = scm.find(".//{*}connection")
            scm_dev_connection = scm.find(".//{*}developerConnection")
            if metadata["scm_url"] is None and scm_url is not None and (scm_url.text or "").strip():
                metadata["scm_url"] = scm_url.text.strip()
            if (
                metadata["scm_connection"] is None
                and scm_connection is not None
                and (scm_connection.text or "").strip()
            ):
                metadata["scm_connection"] = scm_connection.text.strip()
            if (
                metadata["scm_developer_connection"] is None
                and scm_dev_connection is not None
                and (scm_dev_connection.text or "").strip()
            ):
                metadata["scm_developer_connection"] = scm_dev_connection.text.strip()

        issue_management = root.find(".//{*}issueManagement")
        if issue_management is not None:
            issue_url = issue_management.find(".//{*}url")
            if (
                metadata["issue_management_url"] is None
                and issue_url is not None
                and (issue_url.text or "").strip()
            ):
                metadata["issue_management_url"] = issue_url.text.strip()

        return metadata

    def fetch_package_metadata(self, package_name: str) -> RawPackageMetadata:
        group_id, artifact_id = self._parse_coordinates(package_name)

        response = requests.get(
            self.SEARCH_URL,
            params={
                "q": f'g:"{group_id}" AND a:"{artifact_id}"',
                "rows": 1,
                "wt": "json",
            },
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()
        search_payload = response.json()
        docs = ((search_payload.get("response") or {}).get("docs") or [])
        latest_doc = docs[0] if docs else {}
        latest_version = latest_doc.get("latestVersion")

        latest_pom = {}
        pom_payload = None
        if isinstance(latest_version, str) and latest_version.strip():
            pom_url = self._build_pom_url(group_id, artifact_id, latest_version)
            pom_response = requests.get(
                pom_url,
                timeout=settings.request_timeout_seconds,
            )
            if pom_response.ok:
                pom_payload = pom_response.text
                try:
                    latest_pom = self._extract_pom_metadata(pom_payload)
                except ElementTree.ParseError:
                    latest_pom = {}

        compact_payload = {
            "registry_name": "maven",
            "source_reference_url": self.SEARCH_URL,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "coordinates": {
                "group_id": group_id,
                "artifact_id": artifact_id,
            },
            "latest_doc": latest_doc,
            "latest_pom": latest_pom,
        }

        return RawPackageMetadata(
            registry_name="maven",
            package_name=package_name,
            package_version=latest_version,
            raw_metadata=compact_payload,
            candidate_repo_urls=extract_repo_candidates_from_maven(compact_payload),
            full_raw_metadata={
                "search_response": search_payload,
                "pom": pom_payload,
            },
        )
