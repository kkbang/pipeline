import logging
from typing import Any

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency guard
    missing_name = getattr(exc, "name", "") or "dependency"
    raise RuntimeError(
        "FastAPI API requires additional runtime dependencies. "
        f"Missing module: {missing_name}. "
        "Install with: python3 -m pip install fastapi uvicorn"
    ) from exc

from worker.repo.chunking.local_query_repo_service import prepare_local_query_repo
from worker.retrieval.hybrid_chunk_retrieval_service import retrieve_hybrid_candidates_for_source_chunks
from worker.retrieval.user_report_payload import build_user_facing_repo_hybrid_payload
from worker.common.config import settings
from worker.storage.opensearch_store import OpenSearchStore


logger = logging.getLogger(__name__)


app = FastAPI(
    title="License Review Candidate Retrieval API",
    version="1.0.0",
    description=(
        "Process a direct GitHub repository URL and return similar external code "
        "candidates that may require license review. This API does not make a legal "
        "violation determination."
    ),
)

cors_allow_origins = list(settings.retrieval_api_cors_allow_origins) or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class HybridRepoRetrieveRequest(BaseModel):
    repo_url: str = Field(..., min_length=1, description="GitHub repository URL.")
    rule_based_top_k: int = Field(default=50, ge=1, le=200)
    per_variant_k: int = Field(default=20, ge=1, le=100)
    knn_top_k: int = Field(default=50, ge=1, le=200)
    merged_top_k: int = Field(default=100, ge=1, le=500)
    include_same_repo: bool = Field(default=False)
    skip_validation: bool = Field(default=False)


def _exception_detail(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/retrieve/hybrid/by-repo-url")
def retrieve_hybrid_by_repo_url(request: HybridRepoRetrieveRequest) -> dict[str, Any]:
    try:
        process_result = prepare_local_query_repo(
            request.repo_url,
            precompute_embeddings=True,
        )
        store = OpenSearchStore()
        retrieval_result = retrieve_hybrid_candidates_for_source_chunks(
            list(process_result["source_chunks"]),
            store=store,
            rule_based_top_k=request.rule_based_top_k,
            per_variant_k=request.per_variant_k,
            knn_top_k=request.knn_top_k,
            merged_top_k=request.merged_top_k,
            include_same_repo=request.include_same_repo,
        )
        return build_user_facing_repo_hybrid_payload(process_result, retrieval_result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime integration path
        logger.exception(
            "Hybrid retrieval API failed for repo_url=%s",
            request.repo_url,
        )
        raise HTTPException(status_code=500, detail=_exception_detail(exc)) from exc
