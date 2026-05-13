#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


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

from worker.common.config import settings  # noqa: E402
from worker.retrieval.chunk_retrieval_service import (  # noqa: E402
    retrieve_similar_chunks,
    retrieve_similar_chunks_by_chunk_id,
)


class OpenSearchHttpStore:
    def __init__(self) -> None:
        scheme = "https" if settings.opensearch_use_ssl else "http"
        host = str(settings.opensearch_host or "").strip()
        if not host:
            raise RuntimeError("OPENSEARCH_HOST is required")

        self.base_url = f"{scheme}://{host}:{settings.opensearch_port}"
        self.auth = None
        if settings.opensearch_user and settings.opensearch_password:
            self.auth = (settings.opensearch_user, settings.opensearch_password)
        self.verify = bool(settings.opensearch_use_ssl)
        self.timeout_seconds = max(1, int(settings.request_timeout_seconds))

    def _request(
        self,
        *,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = requests.request(
            method=method,
            url=f"{self.base_url}{path}",
            auth=self.auth,
            json=json_body,
            timeout=self.timeout_seconds,
            verify=self.verify,
        )
        response.raise_for_status()
        if not response.text.strip():
            return {}
        return dict(response.json())

    def get_document(self, collection_name: str, doc_id: str) -> dict:
        encoded_doc_id = quote(doc_id, safe="")
        response = requests.request(
            method="GET",
            url=f"{self.base_url}/{collection_name}/_doc/{encoded_doc_id}",
            auth=self.auth,
            timeout=self.timeout_seconds,
            verify=self.verify,
        )
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        payload = response.json()
        if not payload.get("found", True):
            return {}
        return {
            "_id": payload.get("_id", doc_id),
            "_source": payload.get("_source", {}),
        }

    def search_documents(self, collection_name: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            method="POST",
            path=f"/{collection_name}/_search",
            json_body=body,
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

    store = OpenSearchHttpStore()

    if args.chunk_id.strip():
        result = retrieve_similar_chunks_by_chunk_id(
            args.chunk_id.strip(),
            store=store,
            top_k=max(1, args.top_k),
            per_variant_k=max(1, args.per_variant_k),
        )
    else:
        source_doc = _load_input_json(args.input_json.strip())
        result = retrieve_similar_chunks(
            source_doc,
            store=store,
            top_k=max(1, args.top_k),
            per_variant_k=max(1, args.per_variant_k),
        )

    print(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
