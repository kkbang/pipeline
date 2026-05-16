import asyncio
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx

from worker.common.config import settings
from worker.repo.chunking.code_chunk_embedding_service import get_code_chunk_embedding_client
from worker.repo.chunking.repo_chunk_service import _chunk_parameters, _chunk_single_file
from worker.repo.common.identity import build_github_repo_id
from worker.repo.extraction.repo_file_extract_service import _classify_file, _iter_snapshot_files
from worker.repo.snapshot.repo_crawl_service import (
    _build_archive_url,
    _build_async_client,
    _clear_existing_download_path,
    _clear_existing_extract_dir,
    _extract_repo_tar,
    _stream_download_async,
)
from worker.repo.snapshot.repo_snapshot_local_paths import resolve_extracted_root
from worker.retrieval.source_chunk_selection import select_source_chunks
from worker.seed.resolvers.github_url_canonicalizer import canonicalize_github_repo_url


async def _download_repo_snapshot_to_paths(
    *,
    owner: str,
    repo_name: str,
    download_path: Path,
    extract_dir: Path,
) -> dict[str, Any]:
    last_error: Exception | None = None
    async with _build_async_client(1) as client:
        for ref in ("main", "master"):
            archive_url = _build_archive_url(owner, repo_name, ref)
            try:
                await asyncio.to_thread(_clear_existing_download_path, download_path)
                await _stream_download_async(client, archive_url, download_path)
                await asyncio.to_thread(_clear_existing_extract_dir, extract_dir)
                await asyncio.to_thread(_extract_repo_tar, download_path, extract_dir, "r:gz")
                extracted_root = await asyncio.to_thread(resolve_extracted_root, extract_dir)
                return {
                    "snapshot_ref": ref,
                    "snapshot_archive_url": archive_url,
                    "snapshot_root": extracted_root,
                }
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    last_error = exc
                    continue
                raise

    if last_error is not None:
        raise last_error
    raise ValueError(f"No candidate refs available for {owner}/{repo_name}")


def _chunk_snapshot_into_source_chunks(
    *,
    snapshot_root: Path,
    repo_id: str,
    owner: str,
    repo_name: str,
    canonical_repo_url: str,
    snapshot_ref: str,
    source_chunk_limit: int | None,
) -> list[dict[str, Any]]:
    max_lines, overlap_lines = _chunk_parameters()
    chunked_at = datetime.now(timezone.utc).isoformat()
    source_chunks: list[dict[str, Any]] = []

    for absolute_path in _iter_snapshot_files(snapshot_root):
        relative_path = absolute_path.relative_to(snapshot_root).as_posix()
        language, is_code_file, _is_license, _kind = _classify_file(relative_path)
        if not is_code_file:
            continue

        outcome = _chunk_single_file(
            snapshot_root=snapshot_root,
            repo_id=repo_id,
            owner=owner,
            repo_name=repo_name,
            repo_url=canonical_repo_url,
            snapshot_ref=snapshot_ref,
            file_source={
                "file_path": relative_path,
                "language": language,
            },
            max_lines=max_lines,
            overlap_lines=overlap_lines,
            chunked_at=chunked_at,
            use_cached_parser=True,
        )
        if not outcome.chunk_docs:
            continue

        for chunk_doc_id, source in outcome.chunk_docs:
            source_chunks.append({"chunk_id": chunk_doc_id, **dict(source)})

    return select_source_chunks(source_chunks, limit=source_chunk_limit)


def prepare_local_query_repo(
    repo_url: str,
    *,
    source_chunk_limit: int | None = None,
    precompute_embeddings: bool = True,
) -> dict[str, Any]:
    canonicalized = canonicalize_github_repo_url(repo_url)
    if canonicalized is None:
        raise ValueError(f"unsupported GitHub repository URL: {repo_url}")

    owner, repo_name, canonical_repo_url = canonicalized
    repo_id = build_github_repo_id(owner, repo_name)
    snapshot_root: Path | None = None

    with TemporaryDirectory(prefix=f"query-repo-{owner.lower()}-{repo_name.lower()}-") as temp_dir:
        temp_root = Path(temp_dir)
        download_path = temp_root / "snapshot.tar.gz"
        extract_dir = temp_root / "snapshot"
        snapshot_metadata = asyncio.run(
            _download_repo_snapshot_to_paths(
                owner=owner.lower(),
                repo_name=repo_name.lower(),
                download_path=download_path,
                extract_dir=extract_dir,
            )
        )
        snapshot_root = Path(snapshot_metadata["snapshot_root"]).resolve()
        source_chunks = _chunk_snapshot_into_source_chunks(
            snapshot_root=snapshot_root,
            repo_id=repo_id,
            owner=owner.lower(),
            repo_name=repo_name.lower(),
            canonical_repo_url=canonical_repo_url,
            snapshot_ref=str(snapshot_metadata["snapshot_ref"]),
            source_chunk_limit=source_chunk_limit,
        )
        if precompute_embeddings and source_chunks:
            embedding_client = get_code_chunk_embedding_client()
            embedding_client.enrich_documents(
                [(str(source_chunk["chunk_id"]), source_chunk) for source_chunk in source_chunks]
            )

    return {
        "repo_id": repo_id,
        "canonical_repo_url": canonical_repo_url,
        "snapshot_ref": snapshot_metadata["snapshot_ref"],
        "snapshot_archive_url": snapshot_metadata["snapshot_archive_url"],
        "source_chunk_count": len(source_chunks),
        "source_chunks": source_chunks,
        "local_snapshot_cleanup": {
            "status": "pruned",
            "error": None,
            "snapshot_root_exists": bool(snapshot_root and snapshot_root.exists()),
        },
    }
