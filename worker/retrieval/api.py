from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query

from worker.retrieval.chunk_retrieval_service import (
    DEFAULT_PER_VARIANT_K,
    DEFAULT_TOP_K,
    retrieve_similar_chunks,
    retrieve_similar_chunks_by_chunk_id,
)


app = FastAPI(
    title="Code Chunk Retrieval API",
    version="1.0.0",
    description="Retrieve similar code chunks from a chunk JSON document or a stored chunk_id.",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/retrieve")
def retrieve_from_chunk(
    source_doc: dict[str, Any] = Body(..., description="Chunk-like source document."),
    top_k: int = Query(DEFAULT_TOP_K, ge=1, le=200),
    per_variant_k: int = Query(DEFAULT_PER_VARIANT_K, ge=1, le=100),
) -> dict[str, Any]:
    try:
        result = retrieve_similar_chunks(
            source_doc,
            top_k=top_k,
            per_variant_k=per_variant_k,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime integration path
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result.as_dict()


@app.post("/retrieve/by-chunk-id/{chunk_id}")
def retrieve_from_chunk_id(
    chunk_id: str,
    top_k: int = Query(DEFAULT_TOP_K, ge=1, le=200),
    per_variant_k: int = Query(DEFAULT_PER_VARIANT_K, ge=1, le=100),
) -> dict[str, Any]:
    try:
        result = retrieve_similar_chunks_by_chunk_id(
            chunk_id,
            top_k=top_k,
            per_variant_k=per_variant_k,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime integration path
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result.as_dict()
