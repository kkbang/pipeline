import logging
import os
import time
from pathlib import Path


logging.basicConfig(
    level=os.getenv("AIRFLOW_LOG_RETENTION_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("airflow_log_retention")


def _env_flag(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


LOG_DIR = Path(os.getenv("AIRFLOW_LOG_RETENTION_DIR", "/opt/airflow/logs"))
MAX_BYTES = int(os.getenv("AIRFLOW_LOG_RETENTION_MAX_BYTES", str(2 * 1024 * 1024 * 1024)))
CHECK_INTERVAL_SECONDS = int(os.getenv("AIRFLOW_LOG_RETENTION_INTERVAL_SECONDS", "300"))
MIN_FILE_AGE_SECONDS = int(os.getenv("AIRFLOW_LOG_RETENTION_MIN_FILE_AGE_SECONDS", "1800"))
ENABLED = _env_flag("AIRFLOW_LOG_RETENTION_ENABLED", "true")


def _iter_log_files() -> list[tuple[float, int, Path]]:
    files: list[tuple[float, int, Path]] = []
    for path in LOG_DIR.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        files.append((stat.st_mtime, stat.st_size, path))
    return files


def _cleanup_empty_dirs() -> None:
    if not LOG_DIR.exists():
        return
    for path in sorted(LOG_DIR.rglob("*"), reverse=True):
        # Airflow creates convenience symlinks like logs/.../latest. They may
        # report as directories, but rmdir on the symlink path fails.
        if not path.is_dir() or path.is_symlink():
            continue
        try:
            next(path.iterdir())
        except StopIteration:
            path.rmdir()
        except FileNotFoundError:
            continue
        except OSError:
            continue


def prune_once() -> None:
    if not LOG_DIR.exists():
        logger.info("Airflow log retention skipped: log dir does not exist: %s", LOG_DIR)
        return

    files = _iter_log_files()
    total_bytes = sum(size for _, size, _ in files)
    if total_bytes <= MAX_BYTES:
        _cleanup_empty_dirs()
        logger.info(
            "Airflow log retention check: total_bytes=%s max_bytes=%s action=skip",
            total_bytes,
            MAX_BYTES,
        )
        return

    now = time.time()
    deleted_files = 0
    deleted_bytes = 0
    for modified_at, size, path in sorted(files, key=lambda item: item[0]):
        if total_bytes <= MAX_BYTES:
            break
        file_age_seconds = now - modified_at
        if file_age_seconds < MIN_FILE_AGE_SECONDS:
            continue
        try:
            path.unlink()
            total_bytes -= size
            deleted_bytes += size
            deleted_files += 1
        except FileNotFoundError:
            continue
        except OSError as exc:
            logger.warning("Failed to delete Airflow log file path=%s error=%s", path, exc)

    _cleanup_empty_dirs()
    logger.info(
        "Airflow log retention complete: deleted_files=%s deleted_bytes=%s remaining_bytes=%s max_bytes=%s",
        deleted_files,
        deleted_bytes,
        total_bytes,
        MAX_BYTES,
    )


def main() -> None:
    if not ENABLED:
        logger.info("Airflow log retention disabled")
        return

    logger.info(
        "Airflow log retention started: dir=%s max_bytes=%s interval_seconds=%s min_file_age_seconds=%s",
        LOG_DIR,
        MAX_BYTES,
        CHECK_INTERVAL_SECONDS,
        MIN_FILE_AGE_SECONDS,
    )
    while True:
        prune_once()
        time.sleep(max(30, CHECK_INTERVAL_SECONDS))


if __name__ == "__main__":
    main()
