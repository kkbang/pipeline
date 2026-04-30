import asyncio
import logging
import shutil
import tarfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx

from worker.common.config import settings
from worker.common.repo_size_tiering import (
    classify_repo_size_kb,
    parse_repo_size_kb,
    repo_size_tier_rank,
)
from worker.repo.repo_pipeline_manifest_service import (
    CRAWL_DOWNLOADED_STAGE,
    repo_id_shard_index,
    write_stage_manifest,
)
from worker.repo.repo_snapshot_local_paths import (
    normalize_repo_identity,
    resolve_extracted_root,
    resolve_snapshot_paths,
    strip_local_snapshot_fields,
)
from worker.storage.opensearch_store import OpenSearchStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RepoSnapshotCrawlResult:
    doc_id: str
    owner: str
    repo: str
    repo_size_kb: int = 0
    repo_size_tier: str = "normal"
    snapshot_metadata: dict | None = None
    error: Exception | None = None


@dataclass(slots=True)
class CrawlSemaphores:
    download: asyncio.Semaphore
    extract: asyncio.Semaphore


def _reset_downstream_processing_fields(source: dict) -> dict:
    cleaned = {}
    for key, value in source.items():
        if key.startswith("file_extract_"):
            continue
        if key.startswith("chunk_"):
            continue
        if key.startswith("validation_"):
            continue
        if key.startswith("snapshot_local_cleanup_"):
            continue
        cleaned[key] = value
    return cleaned

def _build_ref_candidates(source: dict) -> list[str]:
    candidates: list[str] = []
    seen = set()

    def _append_candidate(value: object) -> None:
        ref = str(value or "").strip()
        if not ref:
            return
        if ref.startswith("refs/heads/"):
            ref = ref[len("refs/heads/") :]
        if ref in seen:
            return
        seen.add(ref)
        candidates.append(ref)

    _append_candidate(source.get("default_branch"))

    for fallback_ref in ["main", "master"]:
        _append_candidate(fallback_ref)

    return candidates


def _build_archive_url(owner: str, repo: str, ref: str) -> str:
    return f"https://codeload.github.com/{owner}/{repo}/tar.gz/refs/heads/{quote(ref, safe='/')}"


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


def _clear_existing_snapshot(download_path: Path, extract_dir: Path) -> None:
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    if download_path.exists():
        download_path.unlink()


def _clear_existing_extract_dir(extract_dir: Path) -> None:
    if extract_dir.exists():
        shutil.rmtree(extract_dir)


def _clear_existing_download_path(download_path: Path) -> None:
    if download_path.exists():
        download_path.unlink()


def _safe_extract_repo_member_path(base_dir: Path, member_name: str) -> Path:
    target_path = (base_dir / member_name).resolve()
    base_path = base_dir.resolve()
    try:
        target_path.relative_to(base_path)
    except ValueError as exc:
        raise ValueError(f"Unsafe archive member path detected: {member_name}")
    return target_path


def _extract_repo_tar(archive_path: Path, extract_dir: Path, mode: str) -> None:
    with tarfile.open(archive_path, mode) as archive:
        for member in archive:
            member_name = str(member.name or "").strip()
            if not member_name:
                continue

            target_path = _safe_extract_repo_member_path(extract_dir, member_name)

            if member.isdir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            # Repository snapshots often contain symlinks, hardlinks, sockets, and
            # other special members that are not required for source analysis and
            # frequently break extraction. Skip them instead of failing the repo.
            if not member.isfile():
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            with extracted, target_path.open("wb") as destination:
                shutil.copyfileobj(extracted, destination)

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

    repo_docs = sorted(
        repo_docs_by_id.values(),
        key=lambda hit: (
            repo_size_tier_rank(_repo_size_tier_from_source(hit.get("_source", {}))),
            _repo_size_kb_from_source(hit.get("_source", {})),
            str(hit.get("_id") or ""),
        ),
    )
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


def _repo_size_kb_from_source(source: dict) -> int:
    return parse_repo_size_kb(source.get("repo_size_kb"))


def _repo_size_tier_from_source(source: dict) -> str:
    configured_tier = str(source.get("repo_size_tier") or "").strip().lower()
    if configured_tier in {"normal", "whale_hint", "giant_hint"}:
        return configured_tier
    return classify_repo_size_kb(_repo_size_kb_from_source(source))


def _build_crawl_semaphores() -> CrawlSemaphores:
    download_concurrency = max(
        1,
        int(settings.repo_crawl_download_concurrency or settings.repo_crawl_concurrency),
    )
    extract_concurrency = max(1, int(settings.repo_crawl_extract_concurrency))
    return CrawlSemaphores(
        download=asyncio.Semaphore(download_concurrency),
        extract=asyncio.Semaphore(extract_concurrency),
    )


def _repo_tier_counts(repo_docs: list[dict]) -> dict[str, int]:
    counts = {"normal": 0, "whale_hint": 0, "giant_hint": 0}
    for hit in repo_docs:
        tier = _repo_size_tier_from_source(hit.get("_source", {}))
        counts[tier] = counts.get(tier, 0) + 1
    return counts


def _split_repo_docs_for_crawl_phases(
    repo_docs: list[dict],
) -> tuple[list[dict], list[dict]]:
    normal_docs: list[dict] = []
    whale_docs: list[dict] = []

    for hit in repo_docs:
        tier = _repo_size_tier_from_source(hit.get("_source", {}))
        if tier == "normal":
            normal_docs.append(hit)
        else:
            whale_docs.append(hit)

    normal_docs.sort(
        key=lambda hit: (
            _repo_size_kb_from_source(hit.get("_source", {})),
            str(hit.get("_id") or ""),
        )
    )
    whale_docs.sort(
        key=lambda hit: (
            -repo_size_tier_rank(_repo_size_tier_from_source(hit.get("_source", {}))),
            -_repo_size_kb_from_source(hit.get("_source", {})),
            str(hit.get("_id") or ""),
        )
    )

    return normal_docs, whale_docs


def get_pending_repo_crawl_stats() -> dict:
    store = OpenSearchStore()
    now = datetime.now(timezone.utc)
    repo_docs = _load_repo_docs_for_crawl(store, now)
    tier_counts = _repo_tier_counts(repo_docs) if repo_docs else {}
    return {
        "pending_count": len(repo_docs),
        "normal_count": int(tier_counts.get("normal", 0)),
        "whale_hint_count": int(tier_counts.get("whale_hint", 0)),
        "giant_hint_count": int(tier_counts.get("giant_hint", 0)),
    }


def has_pending_repo_crawl_work() -> bool:
    return int(get_pending_repo_crawl_stats().get("pending_count") or 0) > 0


def _claim_repo_docs(
    store: OpenSearchStore,
    repo_docs: list[dict],
    started_at: str,
    *,
    batch_id: str,
) -> dict[str, dict]:
    claimed_docs = {}
    claimed_documents: list[tuple[str, dict]] = []

    for hit in repo_docs:
        doc_id = hit["_id"]
        source = strip_local_snapshot_fields(dict(hit.get("_source", {})))
        attempt_count = int(source.get("crawl_attempt_count") or 0) + 1

        claimed_source = {
            **source,
            "crawl_batch_id": batch_id,
            "crawl_status": "downloading",
            "crawl_started_at": started_at,
            "crawl_attempt_count": attempt_count,
            "crawl_error_message": None,
            "snapshot_ref": None,
            "snapshot_archive_url": None,
        }

        claimed_documents.append((doc_id, claimed_source))
        claimed_docs[doc_id] = {"_id": doc_id, "_source": claimed_source}

    if claimed_documents:
        store.bulk_index_documents(
            collection_name="repo_registry_index",
            documents=claimed_documents,
            refresh=False,
            chunk_size=max(1, int(settings.opensearch_bulk_flush_docs)),
        )

    return claimed_docs


async def _download_snapshot_for_ref(
    client: httpx.AsyncClient,
    base_dir: Path,
    owner: str,
    repo: str,
    ref: str,
    download_semaphore: asyncio.Semaphore,
    extract_semaphore: asyncio.Semaphore,
) -> dict:
    download_path, extract_dir = resolve_snapshot_paths(base_dir, owner, repo)
    archive_url = _build_archive_url(owner, repo, ref)

    async with download_semaphore:
        await asyncio.to_thread(_clear_existing_download_path, download_path)
        await _stream_download_async(client, archive_url, download_path)

    async with extract_semaphore:
        await asyncio.to_thread(_clear_existing_extract_dir, extract_dir)
        await asyncio.to_thread(_extract_repo_tar, download_path, extract_dir, "r:gz")
        extracted_root = await asyncio.to_thread(resolve_extracted_root, extract_dir)

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
    download_semaphore: asyncio.Semaphore,
    extract_semaphore: asyncio.Semaphore,
) -> dict:
    owner, repo = normalize_repo_identity(source)
    if not owner or not repo:
        raise ValueError("repo_registry_index document is missing owner or repo_name")

    last_error: Exception | None = None
    for ref in _build_ref_candidates(source):
        try:
            return await _download_snapshot_for_ref(
                client,
                base_dir,
                owner,
                repo,
                ref,
                download_semaphore,
                extract_semaphore,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                last_error = exc
                continue
            raise

    if last_error is not None:
        raise last_error

    raise ValueError(f"No candidate refs available for {owner}/{repo}")


def _mark_repo_downloaded(
    *,
    base_dir: Path,
    doc_id: str,
    claimed_source: dict,
    snapshot_metadata: dict,
    batch_id: str,
) -> None:
    store = OpenSearchStore(base_dir=base_dir)
    crawl_finished_at = datetime.now(timezone.utc).isoformat()
    reset_source = _reset_downstream_processing_fields(
        strip_local_snapshot_fields(claimed_source)
    )
    downloaded_source = {
        **reset_source,
        "crawl_batch_id": batch_id,
        "crawl_status": "downloaded",
        "crawled_at": crawl_finished_at,
        "crawl_finished_at": crawl_finished_at,
        "snapshot_ref": snapshot_metadata.get("ref"),
        "snapshot_archive_url": snapshot_metadata.get("archive_url"),
        "crawl_error_message": None,
    }
    store.replace_document(
        collection_name="repo_registry_index",
        doc_id=doc_id,
        source=downloaded_source,
        refresh=False,
    )


async def _crawl_single_repo(
    client: httpx.AsyncClient,
    base_dir: Path,
    hit: dict,
    semaphores: CrawlSemaphores,
    batch_id: str,
) -> RepoSnapshotCrawlResult:
    doc_id = hit["_id"]
    source = hit["_source"]
    owner, repo = normalize_repo_identity(source)
    repo_size_kb = _repo_size_kb_from_source(source)
    repo_size_tier = _repo_size_tier_from_source(source)
    snapshot_metadata: dict | None = None
    try:
        snapshot_metadata = await _download_repo_snapshot(
            client,
            base_dir,
            source,
            semaphores.download,
            semaphores.extract,
        )
        await asyncio.to_thread(
            _mark_repo_downloaded,
            base_dir=base_dir,
            doc_id=doc_id,
            claimed_source=source,
            snapshot_metadata=snapshot_metadata,
            batch_id=batch_id,
        )

        return RepoSnapshotCrawlResult(
            doc_id=doc_id,
            owner=owner,
            repo=repo,
            repo_size_kb=repo_size_kb,
            repo_size_tier=repo_size_tier,
            snapshot_metadata=snapshot_metadata,
        )
    except Exception as exc:  # noqa: BLE001 - repo별 실패를 결과로 모아야 함
        logger.warning(
            "Snapshot crawl failed for repo=%s/%s tier=%s repo_size_kb=%s error=%s",
            owner,
            repo,
            repo_size_tier,
            repo_size_kb,
            str(exc),
        )
        return RepoSnapshotCrawlResult(
            doc_id=doc_id,
            owner=owner,
            repo=repo,
            repo_size_kb=repo_size_kb,
            repo_size_tier=repo_size_tier,
            snapshot_metadata=snapshot_metadata,
            error=exc,
        )


async def _crawl_repo_batch(
    repo_docs: list[dict],
    base_dir: Path,
    result_handler: Callable[[RepoSnapshotCrawlResult], None],
    semaphores: CrawlSemaphores,
    *,
    batch_id: str,
    phase_name: str,
) -> None:
    if not repo_docs:
        return

    download_concurrency = max(
        1,
        int(settings.repo_crawl_download_concurrency or settings.repo_crawl_concurrency),
    )

    logger.info(
        "Starting repo crawl phase: batch_id=%s phase=%s count=%s",
        batch_id,
        phase_name,
        len(repo_docs),
    )

    async with _build_async_client(download_concurrency) as client:
        tasks = [
            asyncio.create_task(
                _crawl_single_repo(
                    client=client,
                    base_dir=base_dir,
                    hit=hit,
                    semaphores=semaphores,
                    batch_id=batch_id,
                )
            )
            for hit in repo_docs
        ]

        for task in asyncio.as_completed(tasks):
            result = await task
            result_handler(result)


def repo_crawler(batch_id: str) -> None:
    store = OpenSearchStore()
    crawl_started_at = datetime.now(timezone.utc)
    repo_docs = _load_repo_docs_for_crawl(store, crawl_started_at)
    success_manifest_entries: list[dict] = []
    if not repo_docs:
        write_stage_manifest(
            base_dir=store.base_dir,
            batch_id=batch_id,
            stage_name=CRAWL_DOWNLOADED_STAGE,
            entries=[],
        )
        return

    tier_counts = _repo_tier_counts(repo_docs)
    normal_docs, whale_docs = _split_repo_docs_for_crawl_phases(repo_docs)
    semaphores = _build_crawl_semaphores()
    logger.info(
        "Starting repo crawl batch: batch_id=%s total=%s normal=%s whale_hint=%s giant_hint=%s download_concurrency=%s extract_concurrency=%s phases=%s",
        batch_id,
        len(repo_docs),
        tier_counts.get("normal", 0),
        tier_counts.get("whale_hint", 0),
        tier_counts.get("giant_hint", 0),
        max(1, int(settings.repo_crawl_download_concurrency or settings.repo_crawl_concurrency)),
        max(1, int(settings.repo_crawl_extract_concurrency)),
        2 if normal_docs and whale_docs else 1,
    )
    claimed_docs: dict[str, dict] = {}
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
            crawl_status = "crawl_failed"
            if (
                isinstance(crawl_result.error, httpx.HTTPStatusError)
                and crawl_result.error.response.status_code == 404
            ):
                crawl_status = "crawl_not_found"
            updated_source = {
                **strip_local_snapshot_fields(current_source),
                "crawl_status": crawl_status,
                "crawl_finished_at": crawl_finished_at,
                "crawl_error_message": str(crawl_result.error),
                "snapshot_ref": snapshot_metadata.get("ref"),
                "snapshot_archive_url": snapshot_metadata.get("archive_url"),
            }
            claimed_docs[doc_id]["_source"] = updated_source
            pending_registry_docs.append((doc_id, updated_source))
            if len(pending_registry_docs) >= bulk_flush_docs:
                _flush_registry_docs(refresh=False)
            return

        logger.info(
            "Repository snapshot download completed: repo=%s/%s ref=%s tier=%s repo_size_kb=%s",
            crawl_result.owner,
            crawl_result.repo,
            snapshot_metadata.get("ref"),
            crawl_result.repo_size_tier,
            crawl_result.repo_size_kb,
        )
        success_manifest_entries.append(
            {
                "batch_id": batch_id,
                "repo_id": doc_id,
                "owner": crawl_result.owner,
                "repo_name": crawl_result.repo,
                "repo_size_kb": crawl_result.repo_size_kb,
                "repo_size_tier": crawl_result.repo_size_tier,
                "snapshot_ref": snapshot_metadata.get("ref"),
                "snapshot_archive_url": snapshot_metadata.get("archive_url"),
                "stage_status": "downloaded",
                "recorded_at": crawl_finished_at,
            }
        )

    def _run_phase(phase_name: str, phase_repo_docs: list[dict]) -> None:
        if not phase_repo_docs:
            return

        phase_started_at = datetime.now(timezone.utc).isoformat()
        phase_claimed_docs = _claim_repo_docs(
            store=store,
            repo_docs=phase_repo_docs,
            started_at=phase_started_at,
            batch_id=batch_id,
        )
        claimed_docs.update(phase_claimed_docs)
        asyncio.run(
            _crawl_repo_batch(
                repo_docs=list(phase_claimed_docs.values()),
                base_dir=store.base_dir,
                result_handler=_handle_crawl_result,
                semaphores=semaphores,
                batch_id=batch_id,
                phase_name=phase_name,
            )
        )

    _run_phase("normal", normal_docs)
    _run_phase("whale", whale_docs)
    if pending_registry_docs:
        _flush_registry_docs(refresh=False)
    write_stage_manifest(
        base_dir=store.base_dir,
        batch_id=batch_id,
        stage_name=CRAWL_DOWNLOADED_STAGE,
        entries=success_manifest_entries,
    )
    if success_manifest_entries:
        shard_count = max(1, settings.repo_pipeline_parallelism)
        entries_by_shard: dict[int, list[dict]] = {index: [] for index in range(shard_count)}
        for entry in success_manifest_entries:
            repo_id = str(entry.get("repo_id") or "").strip()
            if not repo_id:
                continue
            entries_by_shard[repo_id_shard_index(repo_id, shard_count)].append(entry)

        for shard_index, shard_entries in entries_by_shard.items():
            write_stage_manifest(
                base_dir=store.base_dir,
                batch_id=batch_id,
                stage_name=CRAWL_DOWNLOADED_STAGE,
                entries=shard_entries,
                shard_index=shard_index,
            )
