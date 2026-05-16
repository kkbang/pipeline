from worker.common.config import settings


CHUNK_PHASE_LIGHT = "light"
CHUNK_PHASE_WHALE = "whale"
VALID_CHUNK_PHASES = {CHUNK_PHASE_LIGHT, CHUNK_PHASE_WHALE}


def parse_non_negative_int(value: object) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def normalize_chunk_phase(value: object, *, default: str = CHUNK_PHASE_LIGHT) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in VALID_CHUNK_PHASES:
        return normalized
    return default


def is_whale_repo(*, total_code_bytes: object, code_file_count: object) -> bool:
    return (
        parse_non_negative_int(total_code_bytes) >= max(1, int(settings.repo_chunk_whale_repo_min_code_bytes))
        or parse_non_negative_int(code_file_count) >= max(1, int(settings.repo_chunk_whale_repo_min_code_files))
    )


def resolve_chunk_phase_for_repo(*, total_code_bytes: object, code_file_count: object) -> str:
    if is_whale_repo(total_code_bytes=total_code_bytes, code_file_count=code_file_count):
        return CHUNK_PHASE_WHALE
    return CHUNK_PHASE_LIGHT
