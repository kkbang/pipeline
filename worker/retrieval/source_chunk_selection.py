from typing import Any, Mapping


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _line_span(source_chunk: Mapping[str, Any]) -> int:
    try:
        start_line = int(source_chunk.get("start_line") or 0)
        end_line = int(source_chunk.get("end_line") or 0)
    except (TypeError, ValueError):
        return 0
    if start_line <= 0 or end_line <= 0 or end_line < start_line:
        return 0
    return end_line - start_line + 1


def _is_eligible_source_chunk(source_chunk: Mapping[str, Any]) -> bool:
    chunk_type = _clean_text(source_chunk.get("chunk_type")).lower()
    if chunk_type not in {"function", "class"}:
        return False
    return _line_span(source_chunk) >= 10


def score_source_chunk(source_chunk: Mapping[str, Any]) -> tuple[int, int]:
    score = 0
    chunk_type = _clean_text(source_chunk.get("chunk_type")).lower()
    symbol_name = _clean_text(source_chunk.get("symbol_name"))
    validation_status = _clean_text(source_chunk.get("validation_status")).lower()
    call_tokens = source_chunk.get("call_tokens") or []
    operator_tokens = source_chunk.get("operator_tokens") or []
    identifier_tokens = source_chunk.get("identifier_tokens") or []

    if chunk_type == "function":
        score += 100
    elif chunk_type == "class":
        score += 20

    if symbol_name:
        score += 30

    if validation_status == "valid":
        score += 10

    if call_tokens:
        score += 10
    if operator_tokens:
        score += 5

    identifier_count = len(identifier_tokens) if isinstance(identifier_tokens, list) else 0
    score += min(identifier_count, 10)

    line_span = _line_span(source_chunk)
    if line_span > 0:
        score += min(line_span, 120)

    return score, line_span


def rank_source_chunks(source_chunks: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized_source_chunks = [
        dict(source_chunk)
        for source_chunk in source_chunks
        if _is_eligible_source_chunk(source_chunk)
    ]
    return sorted(
        normalized_source_chunks,
        key=lambda source_chunk: (
            -score_source_chunk(source_chunk)[0],
            -score_source_chunk(source_chunk)[1],
            _clean_text(source_chunk.get("file_path")),
            _clean_text(source_chunk.get("symbol_name")),
            _clean_text(source_chunk.get("chunk_id")),
        ),
    )


def select_source_chunks(
    source_chunks: list[Mapping[str, Any]],
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    ranked = rank_source_chunks(source_chunks)
    if isinstance(limit, int) and limit > 0:
        return ranked[:limit]
    return ranked
