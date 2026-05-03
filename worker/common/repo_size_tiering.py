from worker.common.config import settings


def parse_repo_size_kb(value: object) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def classify_repo_size_kb(size_kb: int) -> str:
    normalized_size_kb = parse_repo_size_kb(size_kb)
    giant_threshold = max(1, int(settings.repo_giant_hint_size_kb))
    whale_threshold = max(1, int(settings.repo_whale_hint_size_kb))
    if normalized_size_kb >= giant_threshold:
        return "giant_hint"
    if normalized_size_kb >= whale_threshold:
        return "whale_hint"
    return "normal"


def repo_size_tier_rank(size_tier: str) -> int:
    normalized = str(size_tier or "").strip().lower()
    if normalized == "giant_hint":
        return 2
    if normalized == "whale_hint":
        return 1
    return 0
