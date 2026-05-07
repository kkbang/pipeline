import os
import shutil
from pathlib import Path
from typing import Callable

from worker.repo.repo_snapshot_local_paths import (
    RAW_REPO_SNAPSHOT_DIR,
    RAW_REPO_SNAPSHOT_DOWNLOAD_DIR,
    resolve_snapshot_download_path,
    resolve_snapshot_extract_dir,
)

FINAL_SNAPSHOT_CLEANUP_STATUSES = {
    "pruned",
    "skipped",
    "missing_confirmed",
}
SNAPSHOT_CLEANUP_ELIGIBLE_CHUNK_STATUSES = {
    "chunked",
    "chunk_failed",
}


def _is_path_within(path: Path, root_dir: Path) -> bool:
    try:
        path.relative_to(root_dir)
        return True
    except ValueError:
        return False


def _rmtree_force_writable(
    func: Callable[..., object],
    path: str,
    exc_info: tuple[type[BaseException], BaseException, object],
) -> None:
    target = Path(path)
    try:
        if target.exists():
            os.chmod(target, 0o700 if target.is_dir() else 0o600)
        parent = target.parent
        if parent.exists():
            os.chmod(parent, 0o700)
        func(path)
    except Exception:
        raise exc_info[1]


def _unlink_force_writable(target: Path) -> None:
    if target.exists():
        os.chmod(target, 0o600)
    parent = target.parent
    if parent.exists():
        os.chmod(parent, 0o700)
    target.unlink()


def prune_empty_parent_dirs(*, start_dir: Path, stop_dir: Path) -> None:
    current = start_dir.resolve()
    resolved_stop_dir = stop_dir.resolve()

    while current != resolved_stop_dir:
        if not current.exists():
            current = current.parent
            continue
        if not current.is_dir():
            break

        parent = current.parent
        if parent.exists():
            os.chmod(parent, 0o700)
        os.chmod(current, 0o700)

        try:
            current.rmdir()
        except OSError:
            break

        current = parent


def prune_local_snapshot_artifacts(*, base_dir: Path, source: dict) -> tuple[bool, str | None]:
    resolved_base_dir = base_dir.resolve()
    pruned_any = False
    missing_artifacts: list[str] = []

    extract_dir = resolve_snapshot_extract_dir(base_dir, source).resolve()
    if not _is_path_within(extract_dir, resolved_base_dir):
        raise ValueError(
            f"Refusing to prune snapshot_extract_dir outside local data dir: {extract_dir}"
        )
    if extract_dir.exists() and not extract_dir.is_dir():
        raise ValueError(f"snapshot_extract_dir is not a directory: {extract_dir}")
    if extract_dir.exists():
        shutil.rmtree(extract_dir, onerror=_rmtree_force_writable)
        prune_empty_parent_dirs(
            start_dir=extract_dir.parent,
            stop_dir=resolved_base_dir / RAW_REPO_SNAPSHOT_DIR,
        )
        pruned_any = True
    else:
        missing_artifacts.append("snapshot_extract_dir_missing")

    download_path = resolve_snapshot_download_path(base_dir, source).resolve()
    if not _is_path_within(download_path, resolved_base_dir):
        raise ValueError(
            f"Refusing to prune snapshot_download_path outside local data dir: {download_path}"
        )
    if download_path.exists() and not download_path.is_file():
        raise ValueError(f"snapshot_download_path is not a file: {download_path}")
    if download_path.exists():
        _unlink_force_writable(download_path)
        prune_empty_parent_dirs(
            start_dir=download_path.parent,
            stop_dir=resolved_base_dir / RAW_REPO_SNAPSHOT_DOWNLOAD_DIR,
        )
        pruned_any = True
    else:
        missing_artifacts.append("snapshot_download_path_missing")

    cleanup_error = ",".join(missing_artifacts) if missing_artifacts else None
    return pruned_any, cleanup_error


def needs_snapshot_cleanup_retry(source: dict) -> bool:
    if (
        str(source.get("chunk_status") or "").strip()
        not in SNAPSHOT_CLEANUP_ELIGIBLE_CHUNK_STATUSES
    ):
        return False

    cleanup_status = str(source.get("snapshot_local_cleanup_status") or "").strip()
    return cleanup_status not in FINAL_SNAPSHOT_CLEANUP_STATUSES
