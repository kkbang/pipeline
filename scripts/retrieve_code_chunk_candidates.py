#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from worker.retrieval.chunk_retrieval_service import (  # noqa: E402
    retrieve_similar_chunks,
    retrieve_similar_chunks_by_chunk_id,
)


def _load_input_json(path: str) -> dict:
    input_path = Path(path).expanduser().resolve()
    return json.loads(input_path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retrieve similar code chunks from code_chunk_index using a chunk document or chunk_id."
    )
    parser.add_argument(
        "--chunk-id",
        default="",
        help="Existing chunk_id stored in code_chunk_index.",
    )
    parser.add_argument(
        "--input-json",
        default="",
        help="Path to a JSON file containing a chunk-like source document.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="Final merged candidate count to return.",
    )
    parser.add_argument(
        "--per-variant-k",
        type=int,
        default=10,
        help="Per query variant search size.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if bool(args.chunk_id.strip()) == bool(args.input_json.strip()):
        raise SystemExit("Provide exactly one of --chunk-id or --input-json")

    if args.chunk_id.strip():
        result = retrieve_similar_chunks_by_chunk_id(
            args.chunk_id.strip(),
            top_k=max(1, args.top_k),
            per_variant_k=max(1, args.per_variant_k),
        )
    else:
        source_doc = _load_input_json(args.input_json.strip())
        result = retrieve_similar_chunks(
            source_doc,
            top_k=max(1, args.top_k),
            per_variant_k=max(1, args.per_variant_k),
        )

    print(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
