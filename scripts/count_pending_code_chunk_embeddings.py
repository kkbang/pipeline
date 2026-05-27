#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from worker.repo.chunking.code_chunk_embedding_backfill_service import (
    count_pending_code_chunk_embedding_backfill_work,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Count how many eligible code chunks are still missing embeddings."
    )
    parser.add_argument(
        "--repo-id",
        default=None,
        help="Optional repo_id filter, for example github:owner/repo",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the JSON result.",
    )
    args = parser.parse_args()

    result = count_pending_code_chunk_embedding_backfill_work(
        repo_id=args.repo_id,
    )
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2 if args.pretty else None,
            sort_keys=bool(args.pretty),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
