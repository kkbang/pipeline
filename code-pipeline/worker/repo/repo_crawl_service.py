import asyncio
import logging
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

from worker.common.config import settings
from worker.seed.services.benchmark_artifact_fetch_service import _extract_tar
from worker.storage.opensearch_store import OpenSearchStore


RAW_REPO_SNAPSHOT_DOWNLOAD_DIR = "raw/repo_snapshot_download"
RAW_REPO_SNAPSHOT_DIR = "raw/repo_snapshot"

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RepoSnapshotCrawlResult:
    doc_id: str
    owner: str
    repo: str
    snapshot_metadata: dict | None = None
    error: Exception | None = None


def _normalize_repo_identity(source: dict) -> tuple[str, str]:
    owner = str(source.get("owner") or "").strip().lower()
    repo = str(source.get("repo_name") or "").strip().lower()
    return owner, repo


def _build_ref_candidates(source: dict) -> list[str]:
    candidates = []
    default_branch = str(source.get("default_branch") or "").strip()
    if default_branch:
        candidates.append(default_branch)

    for fallback_ref in ["main", "master"]:
        if fallback_ref not in candidates:
            candidates.append(fallback_ref)

    return candidates


def _build_archive_url(owner: str, repo: str, ref: str) -> str:
    return f"https://codeload.github.com/{owner}/{repo}/tar.gz/refs/heads/{ref}"


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _resolve_snapshot_paths(base_dir: Path, owner: str, repo: str) -> tuple[Path, Path]:
    download_path = (
        base_dir
        / RAW_REPO_SNAPSHOT_DOWNLOAD_DIR
        / owner
        / repo
        / "snapshot.tar.gz"
    )
    extract_dir = base_dir / RAW_REPO_SNAPSHOT_DIR / owner / repo
    return download_path, extract_dir


def _clear_existing_snapshot(download_path: Path, extract_dir: Path) -> None:
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    if download_path.exists():
        download_path.unlink()


def _resolve_extracted_root(extract_dir: Path) -> Path:
    children = [child for child in extract_dir.iterdir()]
    if len(children) == 1 and children[0].is_dir():
        return children[0]
    return extract_dir


def _build_async_client(concurrency: int) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(settings.request_timeout_seconds),
        limits=httpx.Limits(
            max_connections=max(concurrency, 1),
            max_keepalive_connections=max(concurrency, 1),
        ),
        follow_redirects=True,
        trust_env=False,
    )


async def _stream_download_async(
    client: httpx.AsyncClient,
    source_url: str,
    destination_path: Path,
) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    async with client.stream("GET", source_url) as response:
        response.raise_for_status()
        with destination_path.open("wb") as output:
            async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)


def _is_stale_downloading(source: dict, now: datetime) -> bool:
    if source.get("crawl_status") != "downloading":
        return False

    started_at = _parse_datetime(source.get("crawl_started_at"))
    if started_at is None:
        return True

    elapsed_seconds = (now - started_at).total_seconds()
    return elapsed_seconds >= settings.repo_crawl_lease_seconds


def _field_term_query(field_name: str, value: str) -> dict:
    return {
        "bool": {
            "should": [
                {"term": {f"{field_name}.keyword": value}},
                {"term": {field_name: value}},
            ],
            "minimum_should_match": 1,
        }
    }


def _iter_repo_docs_by_field(store: OpenSearchStore, field_name: str, value: str):
    yield from store.iterate_documents_by_query(
        collection_name="repo_registry_index",
        query=_field_term_query(field_name, value),
        size=1000,
        sort=[{"_id": "asc"}],
    )


def _load_repo_docs_for_crawl(
    store: OpenSearchStore,
    now: datetime,
) -> list[dict]:
    repo_docs_by_id = {}
    stale_count = 0

    for hit in _iter_repo_docs_by_field(store, "crawl_status", "scheduled"):
        repo_docs_by_id[hit["_id"]] = hit

    for hit in _iter_repo_docs_by_field(store, "crawl_status", "downloading"):
        if _is_stale_downloading(hit.get("_source", {}), now):
            stale_count += 1
            repo_docs_by_id[hit["_id"]] = hit

    for hit in _iter_repo_docs_by_field(store, "crawl_status", "crawl_failed"):
        repo_docs_by_id[hit["_id"]] = hit

    if stale_count > 0:
        logger.warning(
            "Reclaiming stale repo crawl leases: count=%s lease_seconds=%s",
            stale_count,
            settings.repo_crawl_lease_seconds,
        )

    repo_docs = list(repo_docs_by_id.values())
    repo_limit = max(0, int(settings.repo_crawl_repo_limit))
    if repo_limit > 0:
        if len(repo_docs) > repo_limit:
            logger.info(
                "Limiting repo crawl batch: selected=%s total_candidates=%s repo_limit=%s",
                repo_limit,
                len(repo_docs),
                repo_limit,
            )
        return repo_docs[:repo_limit]

    return repo_docs


def _claim_repo_docs(
    store: OpenSearchStore,
    repo_docs: list[dict],
    started_at: str,
) -> dict[str, dict]:
    claimed_docs = {}

    for hit in repo_docs:
        doc_id = hit["_id"]
        source = dict(hit.get("_source", {}))
        attempt_count = int(source.get("crawl_attempt_count") or 0) + 1

        claimed_source = {
            **source,
            "crawl_status": "downloading",
            "crawl_started_at": started_at,
            "crawl_attempt_count": attempt_count,
            "crawl_error_message": None,
            "snapshot_ref": None,
            "snapshot_archive_url": None,
            "snapshot_download_path": None,
            "snapshot_extract_dir": None,
            "snapshot_root_path": None,
        }

        store.replace_document(
            collection_name="repo_registry_index",
            doc_id=doc_id,
            source=claimed_source,
        )
        claimed_docs[doc_id] = {"_id": doc_id, "_source": claimed_source}

    return claimed_docs


async def _download_snapshot_for_ref(
    client: httpx.AsyncClient,
    base_dir: Path,
    owner: str,
    repo: str,
    ref: str,
) -> dict:
    download_path, extract_dir = _resolve_snapshot_paths(base_dir, owner, repo)
    archive_url = _build_archive_url(owner, repo, ref)

    await asyncio.to_thread(_clear_existing_snapshot, download_path, extract_dir)
    await _stream_download_async(client, archive_url, download_path)
    await asyncio.to_thread(_extract_tar, download_path, extract_dir, "r:gz")
    extracted_root = await asyncio.to_thread(_resolve_extracted_root, extract_dir)

    return {
        "archive_url": archive_url,
        "ref": ref,
        "download_path": str(download_path),
        "extract_dir": str(extract_dir),
        "extracted_root": str(extracted_root),
    }


async def _download_repo_snapshot(
    client: httpx.AsyncClient,
    base_dir: Path,
    source: dict,
) -> dict:
    owner, repo = _normalize_repo_identity(source)
    if not owner or not repo:
        raise ValueError("repo_registry_index document is missing owner or repo_name")

    last_error: Exception | None = None
    for ref in _build_ref_candidates(source):
        try:
            return await _download_snapshot_for_ref(client, base_dir, owner, repo, ref)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                last_error = exc
                continue
            raise

    if last_error is not None:
        raise last_error

    raise ValueError(f"No candidate refs available for {owner}/{repo}")


async def _crawl_single_repo(
    client: httpx.AsyncClient,
    base_dir: Path,
    hit: dict,
    semaphore: asyncio.Semaphore,
) -> RepoSnapshotCrawlResult:
    doc_id = hit["_id"]
    source = hit["_source"]
    owner, repo = _normalize_repo_identity(source)

    async with semaphore:
        snapshot_metadata: dict | None = None
        try:
            snapshot_metadata = await _download_repo_snapshot(client, base_dir, source)

            return RepoSnapshotCrawlResult(
                doc_id=doc_id,
                owner=owner,
                repo=repo,
                snapshot_metadata=snapshot_metadata,
            )
        except Exception as exc:  # noqa: BLE001 - repo별 실패를 결과로 모아야 함
            logger.warning("Snapshot crawl failed for repo=%s/%s error=%s", owner, repo, str(exc))
            return RepoSnapshotCrawlResult(
                doc_id=doc_id,
                owner=owner,
                repo=repo,
                snapshot_metadata=snapshot_metadata,
                error=exc,
            )


async def _crawl_repo_batch(
    repo_docs: list[dict],
    base_dir: Path,
    result_handler: Callable[[RepoSnapshotCrawlResult], None],
) -> None:
    concurrency = max(1, settings.repo_crawl_concurrency)
    semaphore = asyncio.Semaphore(concurrency)

    async with _build_async_client(concurrency) as client:
        tasks = [
            asyncio.create_task(
                _crawl_single_repo(
                    client=client,
                    base_dir=base_dir,
                    hit=hit,
                    semaphore=semaphore,
                )
            )
            for hit in repo_docs
        ]

        for task in asyncio.as_completed(tasks):
            result = await task
            result_handler(result)


def repo_crawler() -> None:
    store = OpenSearchStore()
    crawl_started_at = datetime.now(timezone.utc)
    repo_docs = _load_repo_docs_for_crawl(store, crawl_started_at)
    if not repo_docs:
        return

    claimed_docs = _claim_repo_docs(
        store=store,
        repo_docs=repo_docs,
        started_at=crawl_started_at.isoformat(),
    )
    bulk_flush_docs = max(1, int(settings.opensearch_bulk_flush_docs))
    pending_registry_docs: list[tuple[str, dict]] = []

    def _flush_registry_docs(*, refresh: bool) -> None:
        if not pending_registry_docs:
            return
        store.bulk_index_documents(
            collection_name="repo_registry_index",
            documents=pending_registry_docs,
            refresh=refresh,
            chunk_size=bulk_flush_docs,
        )
        pending_registry_docs.clear()

    def _handle_crawl_result(crawl_result: RepoSnapshotCrawlResult) -> None:
        doc_id = crawl_result.doc_id
        current_source = dict(claimed_docs[doc_id]["_source"])
        crawl_finished_at = datetime.now(timezone.utc).isoformat()
        snapshot_metadata = crawl_result.snapshot_metadata or {}

        if crawl_result.error is not None:
            updated_source = {
                **current_source,
                "crawl_status": "crawl_failed",
                "crawl_finished_at": crawl_finished_at,
                "crawl_error_message": str(crawl_result.error),
                "snapshot_ref": snapshot_metadata.get("ref"),
                "snapshot_archive_url": snapshot_metadata.get("archive_url"),
                "snapshot_download_path": snapshot_metadata.get("download_path"),
                "snapshot_extract_dir": snapshot_metadata.get("extract_dir"),
                "snapshot_root_path": snapshot_metadata.get("extracted_root"),
            }
            claimed_docs[doc_id]["_source"] = updated_source
            pending_registry_docs.append((doc_id, updated_source))
            if len(pending_registry_docs) >= bulk_flush_docs:
                _flush_registry_docs(refresh=False)
            return

        updated_source = {
            **current_source,
            "crawl_status": "downloaded",
            "crawled_at": crawl_finished_at,
            "crawl_finished_at": crawl_finished_at,
            "snapshot_ref": snapshot_metadata.get("ref"),
            "snapshot_archive_url": snapshot_metadata.get("archive_url"),
            "snapshot_download_path": snapshot_metadata.get("download_path"),
            "snapshot_extract_dir": snapshot_metadata.get("extract_dir"),
            "snapshot_root_path": snapshot_metadata.get("extracted_root"),
            "crawl_error_message": None,
        }
        claimed_docs[doc_id]["_source"] = updated_source
        pending_registry_docs.append((doc_id, updated_source))
        if len(pending_registry_docs) >= bulk_flush_docs:
            _flush_registry_docs(refresh=False)

    asyncio.run(
        _crawl_repo_batch(
            repo_docs=list(claimed_docs.values()),
            base_dir=store.base_dir,
            result_handler=_handle_crawl_result,
        )
    )
    _flush_registry_docs(refresh=False)
    store.refresh_index("repo_registry_index")
