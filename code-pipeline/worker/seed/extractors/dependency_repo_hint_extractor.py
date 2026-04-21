import json
import re
from pathlib import Path

from worker.seed.extractors.github_repo_reference_extractor import (
    extract_github_repo_candidate_urls,
)


MANIFEST_PATHS = [
    # Root-level fallback paths used when repository tree listing is unavailable.
    "requirements.txt",
    "pyproject.toml",
    "package.json",
    "go.mod",
    "Cargo.toml",
]

MANIFEST_FILE_NAMES = {
    "requirements.txt",
    "pyproject.toml",
    "setup.py",
    "setup.cfg",
    "Pipfile",
    "Pipfile.lock",
    "poetry.lock",
    "package.json",
    "pnpm-workspace.yaml",
    "go.mod",
    "go.work",
    "Cargo.toml",
    "Cargo.lock",
    "pom.xml",
    "build.gradle",
    "build.gradle.kts",
    "settings.gradle",
    "settings.gradle.kts",
    "Gemfile",
    "gems.rb",
    "composer.json",
    "mix.exs",
    "deps.edn",
    ".gitmodules",
    "WORKSPACE",
    "WORKSPACE.bazel",
    "MODULE.bazel",
}


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


def _manifest_path_score(manifest_path: str) -> tuple[int, int, str]:
    normalized = str(manifest_path).strip().lstrip("./")
    parts = [segment for segment in normalized.split("/") if segment]
    depth = max(0, len(parts) - 1)

    penalty = 0
    lowered = normalized.lower()
    if "vendor/" in lowered or "/vendor/" in lowered:
        penalty += 4
    if "node_modules/" in lowered or "/node_modules/" in lowered:
        penalty += 6
    if "dist/" in lowered or "/dist/" in lowered:
        penalty += 2
    if "build/" in lowered or "/build/" in lowered:
        penalty += 2

    return (depth + penalty, len(normalized), normalized)


def select_manifest_paths_from_repo_paths(
    repo_paths: list[str],
    *,
    max_paths: int = 30,
) -> list[str]:
    if not isinstance(repo_paths, list) or not repo_paths:
        return []

    candidates = []
    for repo_path in repo_paths:
        if not isinstance(repo_path, str):
            continue
        normalized = repo_path.strip().lstrip("./")
        if not normalized:
            continue

        file_name = Path(normalized).name
        lowered = normalized.lower()
        is_workflow_file = lowered.startswith(".github/workflows/") and (
            lowered.endswith(".yml") or lowered.endswith(".yaml")
        )
        if file_name not in MANIFEST_FILE_NAMES and not is_workflow_file:
            continue

        candidates.append(normalized)

    if not candidates:
        return []

    deduped = _unique_urls(candidates)
    deduped.sort(key=_manifest_path_score)
    safe_max_paths = max(1, int(max_paths))
    return deduped[:safe_max_paths]


def extract_repo_candidates_from_dependency_manifest(manifest_path: str, manifest_text: str) -> list[str]:
    urls = extract_github_repo_candidate_urls(manifest_text)

    normalized_manifest_path = str(manifest_path).strip().lstrip("./")
    lowered_manifest_path = normalized_manifest_path.lower()
    manifest_name = Path(manifest_path).name
    if manifest_name == "package.json":
        urls.extend(_extract_package_json_shorthands(manifest_text))
    elif lowered_manifest_path.startswith(".github/workflows/") and (
        lowered_manifest_path.endswith(".yml") or lowered_manifest_path.endswith(".yaml")
    ):
        urls.extend(_extract_github_actions_uses(manifest_text))

    return _unique_urls(urls)


_GITHUB_ACTIONS_USES_PATTERN = re.compile(
    r"(?mi)^\s*uses:\s*([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)@",
)


def _extract_github_actions_uses(manifest_text: str) -> list[str]:
    urls = []
    for match in _GITHUB_ACTIONS_USES_PATTERN.findall(manifest_text):
        owner_repo = (match or "").strip()
        if owner_repo:
            urls.append(f"https://github.com/{owner_repo}")
    return urls
