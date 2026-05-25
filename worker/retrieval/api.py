import asyncio
import logging
from time import perf_counter
from typing import Any

try:
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
    from starlette.responses import JSONResponse
    from starlette.concurrency import run_in_threadpool
except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency guard
    missing_name = getattr(exc, "name", "") or "dependency"
    raise RuntimeError(
        "FastAPI API requires additional runtime dependencies. "
        f"Missing module: {missing_name}. "
        "Install with: python3 -m pip install fastapi uvicorn"
    ) from exc

from worker.repo.chunking.local_query_repo_service import prepare_local_query_repo
from worker.retrieval.concurrency_limit import NonBlockingConcurrencyLimiter
from worker.retrieval.rate_limit import FixedWindowRateLimiter
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
_retrieve_request_concurrency_limiter = NonBlockingConcurrencyLimiter(
    limit=settings.retrieval_api_max_concurrent_requests
)
_retrieve_rate_limiter = FixedWindowRateLimiter(
    limit=settings.retrieval_api_rate_limit_requests,
    window_seconds=settings.retrieval_api_rate_limit_window_seconds,
)
_RETRIEVE_BY_REPO_URL_PATH = "/retrieve/hybrid/by-repo-url"
_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "same-origin",
    "Cache-Control": "no-store",
}


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

def _retrieve_hybrid_by_repo_url_sync(request: HybridRepoRetrieveRequest) -> dict[str, Any]:
    total_started_at = perf_counter()
    prepare_started_at = perf_counter()
    process_result = prepare_local_query_repo(
        request.repo_url,
        precompute_embeddings=True,
    )
    prepare_elapsed_seconds = round(perf_counter() - prepare_started_at, 4)

    retrieval_started_at = perf_counter()
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
    retrieval_elapsed_seconds = round(perf_counter() - retrieval_started_at, 4)

    payload_started_at = perf_counter()
    payload = build_user_facing_repo_hybrid_payload(process_result, retrieval_result)
    payload_elapsed_seconds = round(perf_counter() - payload_started_at, 4)

    payload["timings"] = {
        "total_seconds": round(perf_counter() - total_started_at, 4),
        "prepare_local_query_repo": {
            "total_seconds": prepare_elapsed_seconds,
            **dict(process_result.get("timings") or {}),
        },
        "retrieve_hybrid_candidates": {
            "total_seconds": retrieval_elapsed_seconds,
            **dict(retrieval_result.get("timings") or {}),
        },
        "build_user_facing_payload": {
            "total_seconds": payload_elapsed_seconds,
        },
    }
    logger.info(
        "Hybrid retrieval request timings repo_url=%s timings=%s",
        request.repo_url,
        payload["timings"],
    )
    return payload


def _apply_security_headers(response) -> None:
    for header_name, header_value in _SECURITY_HEADERS.items():
        response.headers.setdefault(header_name, header_value)


def _request_client_key(request: Request) -> str:
    forwarded_for = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip() or "unknown"
    if request.client and request.client.host:
        return str(request.client.host).strip() or "unknown"
    return "unknown"


def _apply_rate_limit_headers(response, *, limit: int, remaining: int, reset_after_seconds: int) -> None:
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
    response.headers["X-RateLimit-Reset"] = str(max(0, reset_after_seconds))


def _apply_concurrency_headers(response, *, limit: int, in_flight: int, remaining: int) -> None:
    response.headers["X-Concurrency-Limit"] = str(max(0, limit))
    response.headers["X-Concurrency-In-Flight"] = str(max(0, in_flight))
    response.headers["X-Concurrency-Remaining"] = str(max(0, remaining))


@app.middleware("http")
async def retrieval_api_middleware(request: Request, call_next):
    if request.url.path != _RETRIEVE_BY_REPO_URL_PATH:
        response = await call_next(request)
        _apply_security_headers(response)
        return response

    if request.method != "POST":
        response = await call_next(request)
        _apply_security_headers(response)
        return response

    content_type = str(request.headers.get("content-type") or "").lower()
    if "application/json" not in content_type:
        response = JSONResponse(
            status_code=415,
            content={"detail": "Content-Type must be application/json"},
        )
        _apply_security_headers(response)
        return response

    request_body = await request.body()
    if len(request_body) > max(1, int(settings.retrieval_api_max_request_body_bytes)):
        response = JSONResponse(
            status_code=413,
            content={"detail": "Request body too large"},
        )
        _apply_security_headers(response)
        return response

    concurrency_decision, reservation = await _retrieve_request_concurrency_limiter.try_acquire()
    if reservation is None:
        response = JSONResponse(
            status_code=503,
            content={"detail": "Too many in-flight retrieval requests"},
        )
        response.headers["Retry-After"] = "5"
        _apply_concurrency_headers(
            response,
            limit=concurrency_decision.limit,
            in_flight=concurrency_decision.in_flight,
            remaining=concurrency_decision.remaining,
        )
        _apply_security_headers(response)
        return response

    rate_limit_decision = _retrieve_rate_limiter.check(_request_client_key(request))
    if not rate_limit_decision.allowed:
        response = JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded"},
        )
        response.headers["Retry-After"] = str(rate_limit_decision.retry_after_seconds)
        _apply_rate_limit_headers(
            response,
            limit=rate_limit_decision.limit,
            remaining=rate_limit_decision.remaining,
            reset_after_seconds=rate_limit_decision.reset_after_seconds,
        )
        _apply_concurrency_headers(
            response,
            limit=concurrency_decision.limit,
            in_flight=concurrency_decision.in_flight,
            remaining=concurrency_decision.remaining,
        )
        _apply_security_headers(response)
        await reservation.release()
        return response

    async with reservation:
        response = await call_next(request)

    _apply_rate_limit_headers(
        response,
        limit=rate_limit_decision.limit,
        remaining=rate_limit_decision.remaining,
        reset_after_seconds=rate_limit_decision.reset_after_seconds,
    )
    _apply_concurrency_headers(
        response,
        limit=concurrency_decision.limit,
        in_flight=concurrency_decision.in_flight,
        remaining=concurrency_decision.remaining,
    )
    _apply_security_headers(response)
    return response

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/retrieve/hybrid/by-repo-url")
async def retrieve_hybrid_by_repo_url(request: HybridRepoRetrieveRequest) -> dict[str, Any]:
    try:
        return await run_in_threadpool(
            _retrieve_hybrid_by_repo_url_sync,
            request,
        )
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
