#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file(PROJECT_ROOT / ".env")

try:
    from worker.repo.local_query_repo_service import prepare_local_query_repo  # noqa: E402
    from worker.retrieval.hybrid_chunk_retrieval_service import (  # noqa: E402
        find_repo_chunk,
        retrieve_hybrid_candidates,
        retrieve_hybrid_candidates_by_chunk_id,
        retrieve_hybrid_candidates_for_repo,
        retrieve_hybrid_candidates_for_source_chunks,
    )
    from worker.retrieval.user_report_payload import (  # noqa: E402
        build_user_facing_hybrid_result_payload,
        build_user_facing_repo_hybrid_payload,
    )
    from worker.storage.opensearch_store import OpenSearchStore  # noqa: E402
except ModuleNotFoundError as exc:  # pragma: no cover - import-time dependency guard
    if exc.name == "opensearchpy":
        raise SystemExit(
            "Missing dependency 'opensearchpy'. Install runtime deps first:\n"
            "  python3 -m pip install -r airflow/requirements.txt\n"
            "or minimally:\n"
            "  python3 -m pip install opensearch-py==2.7.1"
        ) from exc
    raise


def _load_input_json(path: str) -> dict[str, Any]:
    input_path = Path(path).expanduser().resolve()
    return json.loads(input_path.read_text(encoding="utf-8"))


def _flatten_query_repo_payload(payload: dict[str, Any], process_result: dict[str, Any]) -> dict[str, Any]:
    merged_payload = dict(payload)
    merged_payload["repo_id"] = merged_payload.get("repo_id") or process_result.get("repo_id")
    merged_payload["canonical_repo_url"] = process_result.get("canonical_repo_url")
    merged_payload["local_snapshot_cleanup"] = process_result.get("local_snapshot_cleanup")
    return merged_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run direct repo processing optionally, then retrieve hybrid rule-based + kNN code chunk candidates."
    )
    parser.add_argument("--repo-url", default="", help="Optional GitHub repository URL to process before retrieval.")
    parser.add_argument("--chunk-id", default="", help="Existing chunk_id stored in code_chunk_index.")
    parser.add_argument("--repo-id", default="", help="Source repo_id when resolving a chunk by file_path.")
    parser.add_argument("--file-path", default="", help="Source file_path when resolving a chunk by repo_id.")
    parser.add_argument("--symbol-name", default="", help="Optional source symbol_name when resolving a chunk by repo_id/file_path.")
    parser.add_argument("--input-json", default="", help="Path to a JSON file containing a chunk-like source document.")
    parser.add_argument("--skip-validation", action="store_true", help="Skip repo validation when processing --repo-url.")
    parser.add_argument("--rule-based-top-k", type=int, default=50, help="Rule-based candidate cap.")
    parser.add_argument("--per-variant-k", type=int, default=20, help="Per-variant search size for rule-based retrieval.")
    parser.add_argument("--knn-top-k", type=int, default=50, help="kNN candidate cap.")
    parser.add_argument("--merged-top-k", type=int, default=100, help="Merged candidate cap.")
    parser.add_argument("--source-chunk-limit", type=int, default=0, help="Optional cap for source chunks when retrieving an entire repo.")
    parser.add_argument("--include-same-repo", action="store_true", help="Include same-repo candidates.")
    parser.add_argument("--verbose", action="store_true", help="Emit full candidate payloads.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    store = OpenSearchStore()

    process_result: dict[str, Any] | None = None
    resolved_repo_id = args.repo_id.strip()
    if args.repo_url.strip():
        process_result = prepare_local_query_repo(
            args.repo_url.strip(),
            source_chunk_limit=(args.source_chunk_limit if args.source_chunk_limit > 0 else None),
            precompute_embeddings=True,
        )

    if args.input_json.strip():
        source_doc = _load_input_json(args.input_json.strip())
        result = retrieve_hybrid_candidates(
            source_doc,
            store=store,
            rule_based_top_k=args.rule_based_top_k,
            per_variant_k=args.per_variant_k,
            knn_top_k=args.knn_top_k,
            merged_top_k=args.merged_top_k,
            include_same_repo=args.include_same_repo,
        )
    else:
        resolved_chunk_id = args.chunk_id.strip()
        if not resolved_chunk_id and resolved_repo_id and args.file_path.strip():
            chunk_info = find_repo_chunk(
                repo_id=resolved_repo_id,
                file_path=args.file_path.strip(),
                symbol_name=args.symbol_name.strip(),
                store=store,
            )
            if chunk_info is None:
                raise SystemExit(
                    f"Could not resolve a chunk for repo_id={resolved_repo_id} file_path={args.file_path.strip()} symbol_name={args.symbol_name.strip() or '<any>'}"
            )
            resolved_chunk_id = str(chunk_info["chunk_id"])

        if process_result is not None:
            repo_result = retrieve_hybrid_candidates_for_source_chunks(
                list(process_result["source_chunks"]),
                store=store,
                rule_based_top_k=args.rule_based_top_k,
                per_variant_k=args.per_variant_k,
                knn_top_k=args.knn_top_k,
                merged_top_k=args.merged_top_k,
                include_same_repo=args.include_same_repo,
            )
            payload = (
                _flatten_query_repo_payload(dict(repo_result), process_result)
                if args.verbose
                else build_user_facing_repo_hybrid_payload(process_result, repo_result)
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        if not resolved_chunk_id and resolved_repo_id:
            repo_result = retrieve_hybrid_candidates_for_repo(
                resolved_repo_id,
                store=store,
                source_chunk_limit=(args.source_chunk_limit if args.source_chunk_limit > 0 else None),
                rule_based_top_k=args.rule_based_top_k,
                per_variant_k=args.per_variant_k,
                knn_top_k=args.knn_top_k,
                merged_top_k=args.merged_top_k,
                include_same_repo=args.include_same_repo,
            )
            payload = (
                repo_result
                if args.verbose
                else build_user_facing_repo_hybrid_payload(
                    {
                        "repo_id": repo_result.get("repo_id"),
                        "canonical_repo_url": "",
                    },
                    repo_result,
                )
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        if not resolved_chunk_id:
            raise SystemExit(
                "A source selector is required: provide --chunk-id, or --repo-id/--file-path, or --repo-url."
            )

        result = retrieve_hybrid_candidates_by_chunk_id(
            resolved_chunk_id,
            store=store,
            rule_based_top_k=args.rule_based_top_k,
            per_variant_k=args.per_variant_k,
            knn_top_k=args.knn_top_k,
            merged_top_k=args.merged_top_k,
            include_same_repo=args.include_same_repo,
        )

    payload = (
        result.as_dict()
        if args.verbose
        else build_user_facing_hybrid_result_payload(
            result,
            source_chunk={
                "file_path": args.file_path.strip(),
                "symbol_name": args.symbol_name.strip(),
            },
        )
    )
    if process_result is not None:
        payload = _flatten_query_repo_payload(dict(payload), process_result)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
