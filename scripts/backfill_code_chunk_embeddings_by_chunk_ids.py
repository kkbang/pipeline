#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backfill_code_chunk_embeddings import (  # noqa: E402
    CODE_CHUNK_EMBEDDING_INDEX,
    CODE_CHUNK_INDEX,
    EMBEDDING_BACKFILL_SOURCE_FIELDS,
    EMBEDDING_BACKFILL_WRITE_BATCH_SIZE,
    CodeChunkEmbeddingClient,
    EmbeddingBackfillStats,
    OpenSearchHttpStore,
    _filter_missing_embedding_docs,
    _flush_embedding_backfill_batch,
    _is_eligible_source_chunk,
)


def _load_chunk_ids_from_file(path: str) -> list[str]:
    file_path = Path(path).expanduser()
    return [
        line.strip()
        for line in file_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _load_chunk_ids_from_stdin() -> list[str]:
    return [
        line.strip()
        for line in sys.stdin.read().splitlines()
        if line.strip()
    ]


def _dedupe_chunk_ids(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def backfill_code_chunk_embeddings_for_chunk_ids(
    *,
    chunk_ids: list[str],
    store: Any | None = None,
    embedding_client: Any | None = None,
    write_batch_size: int | None = None,
    force_reembed: bool = False,
    refresh_writes: bool = False,
    skip_writes: bool = False,
) -> dict[str, Any]:
    requested_chunk_ids = _dedupe_chunk_ids(chunk_ids)
    resolved_store = store or OpenSearchHttpStore()
    if hasattr(resolved_store, "ensure_index_created"):
        resolved_store.ensure_index_created(CODE_CHUNK_EMBEDDING_INDEX)

    resolved_embedding_client = embedding_client or CodeChunkEmbeddingClient()
    if not resolved_embedding_client.enabled:
        raise RuntimeError(
            "code chunk embedding client is not enabled; set CODE_CHUNK_EMBEDDING_ENABLED, "
            "CODE_CHUNK_EMBEDDING_ENDPOINT, and CODE_CHUNK_EMBEDDING_MODEL"
        )

    safe_write_batch_size = max(1, int(write_batch_size or EMBEDDING_BACKFILL_WRITE_BATCH_SIZE))
    docs_by_id = resolved_store.get_documents_by_ids(
        CODE_CHUNK_INDEX,
        requested_chunk_ids,
        source_includes=EMBEDDING_BACKFILL_SOURCE_FIELDS,
    )

    found_docs: list[tuple[str, dict[str, Any]]] = []
    missing_chunk_ids: list[str] = []
    for chunk_id in requested_chunk_ids:
        doc = docs_by_id.get(chunk_id)
        if not isinstance(doc, dict):
            missing_chunk_ids.append(chunk_id)
            continue
        source = dict(doc.get("_source", {}))
        found_docs.append((chunk_id, source))

    eligible_docs = [
        (chunk_id, source)
        for chunk_id, source in found_docs
        if _is_eligible_source_chunk(source)
    ]
    docs_to_process = (
        list(eligible_docs)
        if force_reembed
        else _filter_missing_embedding_docs(resolved_store, eligible_docs)
    )

    stats = EmbeddingBackfillStats(seen_count=len(docs_to_process))
    pending_update_docs: list[tuple[str, dict[str, Any]]] = []
    for start in range(0, len(docs_to_process), safe_write_batch_size):
        batch_docs = docs_to_process[start : start + safe_write_batch_size]
        pending_update_docs.extend(
            _flush_embedding_backfill_batch(
                embedding_client=resolved_embedding_client,
                batch_docs=batch_docs,
                force_reembed=force_reembed,
                stats=stats,
            )
        )

    if not skip_writes and pending_update_docs:
        resolved_store.bulk_upsert_documents(
            collection_name=CODE_CHUNK_EMBEDDING_INDEX,
            documents=pending_update_docs,
            refresh=refresh_writes,
            chunk_size=safe_write_batch_size,
        )

    return {
        "source_index": CODE_CHUNK_INDEX,
        "embedding_index": CODE_CHUNK_EMBEDDING_INDEX,
        "requested_chunk_id_count": len(requested_chunk_ids),
        "found_chunk_id_count": len(found_docs),
        "missing_chunk_id_count": len(missing_chunk_ids),
        "missing_chunk_ids": missing_chunk_ids,
        "eligible_source_chunk_count": len(eligible_docs),
        "selected_for_embedding_count": len(docs_to_process),
        "force_reembed": bool(force_reembed),
        "refresh_writes": bool(refresh_writes),
        "skip_writes": bool(skip_writes),
        **stats.as_dict(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write embeddings for an explicit list of code chunk IDs."
    )
    parser.add_argument(
        "--file",
        default="chunk_id.txt",
        help="File containing newline-delimited chunk IDs. Defaults to ./chunk_id.txt",
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read newline-delimited chunk IDs from stdin.",
    )
    parser.add_argument(
        "--force-reembed",
        action="store_true",
        help="Recompute embeddings even if they already exist in the embedding index.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh the embedding index after writing.",
    )
    parser.add_argument(
        "--skip-writes",
        action="store_true",
        help="Run embedding generation without writing results to OpenSearch.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the JSON result.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    all_chunk_ids: list[str] = []
    if args.file:
        all_chunk_ids.extend(_load_chunk_ids_from_file(args.file))
    if args.stdin:
        all_chunk_ids.extend(_load_chunk_ids_from_stdin())

    result = backfill_code_chunk_embeddings_for_chunk_ids(
        chunk_ids=all_chunk_ids,
        force_reembed=args.force_reembed,
        refresh_writes=args.refresh,
        skip_writes=args.skip_writes,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
