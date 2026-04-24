from pathlib import Path


RAW_REPO_SNAPSHOT_DOWNLOAD_DIR = "raw/repo_snapshot_download"
RAW_REPO_SNAPSHOT_DIR = "raw/repo_snapshot"
LOCAL_SNAPSHOT_SOURCE_FIELDS = (
    "snapshot_download_path",
    "snapshot_extract_dir",
    "snapshot_root_path",
)


def normalize_repo_identity(source: dict) -> tuple[str, str]:
    owner = str(source.get("owner") or "").strip().lower()
    repo = str(source.get("repo_name") or "").strip().lower()
    return owner, repo


def strip_local_snapshot_fields(source: dict) -> dict:
    cleaned = dict(source)
    for field_name in LOCAL_SNAPSHOT_SOURCE_FIELDS:
        cleaned.pop(field_name, None)
    return cleaned


def resolve_snapshot_paths(base_dir: Path, owner: str, repo: str) -> tuple[Path, Path]:
    download_path = (
        base_dir
        / RAW_REPO_SNAPSHOT_DOWNLOAD_DIR
        / owner
        / repo
        / "snapshot.tar.gz"
    )
    extract_dir = base_dir / RAW_REPO_SNAPSHOT_DIR / owner / repo
    return download_path, extract_dir


def resolve_snapshot_download_path(base_dir: Path, source: dict) -> Path:
    owner, repo = normalize_repo_identity(source)
    if not owner or not repo:
        raise ValueError("repo source is missing owner or repo_name")
    download_path, _ = resolve_snapshot_paths(base_dir, owner, repo)
    return download_path


def resolve_snapshot_extract_dir(base_dir: Path, source: dict) -> Path:
    owner, repo = normalize_repo_identity(source)
    if not owner or not repo:
        raise ValueError("repo source is missing owner or repo_name")
    _, extract_dir = resolve_snapshot_paths(base_dir, owner, repo)
    return extract_dir


def resolve_extracted_root(extract_dir: Path) -> Path:
    if not extract_dir.exists() or not extract_dir.is_dir():
        return extract_dir
    children = [child for child in extract_dir.iterdir()]
    if len(children) == 1 and children[0].is_dir():
        return children[0]
    return extract_dir


def resolve_snapshot_root_path(base_dir: Path, source: dict) -> Path:
    extract_dir = resolve_snapshot_extract_dir(base_dir, source)
    return resolve_extracted_root(extract_dir)
