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

    def multi_get_documents(self, collection_name: str, doc_ids: list[str]) -> dict[str, dict[str, Any]]:
        payload = self._request(
            method="POST",
            path=f"/{collection_name}/_mget",
            json_body={"ids": [doc_id for doc_id in dict.fromkeys(doc_ids) if str(doc_id or "").strip()]},
        )
        docs: dict[str, dict[str, Any]] = {}
        for item in payload.get("docs", []):
            doc_id = str(item.get("_id") or "").strip()
            if not doc_id or not item.get("found", False):
                continue
            docs[doc_id] = {
                "_id": doc_id,
                "_source": item.get("_source", {}),
            }
        return docs

    def search_documents(self, collection_name: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            method="POST",
            path=f"/{collection_name}/_search",
            json_body=body,
        )

    def multi_search_documents(self, collection_name: str, bodies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not bodies:
            return []
        payload_lines: list[str] = []
        for body in bodies:
            payload_lines.append(json.dumps({}))
            payload_lines.append(json.dumps(body))
        response = requests.request(
            method="POST",
            url=f"{self.base_url}/{collection_name}/_msearch",
            auth=self.auth,
            data="\n".join(payload_lines) + "\n",
            headers={"Content-Type": "application/x-ndjson"},
            timeout=self.timeout_seconds,
            verify=self.verify,
        )
        response.raise_for_status()
        payload = response.json()
        return list(payload.get("responses", []))


def _load_input_json(path: str) -> dict:
    input_path = Path(path).expanduser().resolve()
    return json.loads(input_path.read_text(encoding="utf-8"))


def _compact_result_payload(result: Any) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for rank, candidate in enumerate(result.candidates, start=1):
        candidates.append(
            {
                "rank": rank,
                "chunk_id": candidate.chunk_id,
                "repo_id": candidate.repo_id,
                "file_path": candidate.file_path,
                "symbol_name": candidate.symbol_name,
                "aggregate_score": round(candidate.aggregate_score, 6),
                "risk_level": candidate.license_review.get("risk_level"),
                "risk_score": candidate.license_review.get("risk_score"),
                "strongest_evidence_type": candidate.license_review.get("strongest_evidence_type"),
                "source_repo": {
                    "repo_url": candidate.source_repo.get("repo_url"),
                    "license_spdx": candidate.source_repo.get("license_spdx"),
                },
                "match_analysis": {
                    "ranking_score": candidate.match_analysis.get("ranking_score"),
                    "domain_alignment_terms": candidate.match_analysis.get("domain_alignment_terms"),
                    "high_signal_domain_terms": candidate.match_analysis.get("high_signal_domain_terms"),
                    "call_token_overlap_count": candidate.match_analysis.get("call_token_overlap_count"),
                    "identifier_term_overlap_count": candidate.match_analysis.get("identifier_term_overlap_count"),
                    "operator_token_overlap_count": candidate.match_analysis.get("operator_token_overlap_count"),
                    "cluster_size": candidate.match_analysis.get("cluster_size", 1),
                },
            }
        )

    return {
        "retrieval_version": result.retrieval_version,
        "source_chunk_id": result.query_bundle.source_chunk_id,
        "source_repo_id": result.query_bundle.source_repo_id,
        "candidate_count": result.candidate_count,
        "license_review_summary": dict(result.license_review_summary),
        "candidates": candidates,
    }


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
    parser.add_argument(
        "--include-same-repo",
        action="store_true",
        help="Include matches from the same source repository.",
    )
    parser.add_argument(
        "--include-low-confidence",
        action="store_true",
        help="Include low-confidence and structural-only candidates for debugging.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print the full retrieval payload including query bundle and source fields.",
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
            include_same_repo=bool(args.include_same_repo),
            include_low_confidence=bool(args.include_low_confidence),
        )
    else:
        source_doc = _load_input_json(args.input_json.strip())
        result = retrieve_similar_chunks(
            source_doc,
            store=store,
            top_k=max(1, args.top_k),
            per_variant_k=max(1, args.per_variant_k),
            include_same_repo=bool(args.include_same_repo),
            include_low_confidence=bool(args.include_low_confidence),
        )

    payload = result.as_dict() if args.verbose else _compact_result_payload(result)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
