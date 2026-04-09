import json
import re
from pathlib import Path

from worker.seed.extractors.github_repo_reference_extractor import (
    extract_github_repo_candidate_urls,
)


MANIFEST_PATHS = [
    "requirements.txt",
    "pyproject.toml",
    "package.json",
    "go.mod",
    "Cargo.toml",
]


def _unique_urls(urls: list[str]) -> list[str]:
    unique = []
    seen = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(url)
    return unique


def _normalize_npm_shorthand(value: str) -> str | None:
    if value.startswith("github:"):
        value = value[len("github:"):]

    value = value.split("#", 1)[0].strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value):
        return None

    return f"https://github.com/{value}"


def _extract_package_json_shorthands(manifest_text: str) -> list[str]:
    try:
        payload = json.loads(manifest_text)
    except json.JSONDecodeError:
        return []

    urls = []
    dependency_sections = [
        "dependencies",
        "devDependencies",
        "peerDependencies",
        "optionalDependencies",
        "overrides",
    ]
    for section in dependency_sections:
        values = payload.get(section) or {}
        if not isinstance(values, dict):
            continue

        for version in values.values():
            if not isinstance(version, str):
                continue

            normalized = _normalize_npm_shorthand(version)
            if normalized:
                urls.append(normalized)

    return urls


def extract_repo_candidates_from_dependency_manifest(manifest_path: str, manifest_text: str) -> list[str]:
    urls = extract_github_repo_candidate_urls(manifest_text)

    if Path(manifest_path).name == "package.json":
        urls.extend(_extract_package_json_shorthands(manifest_text))

    return _unique_urls(urls)
