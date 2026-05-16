from datetime import datetime, timezone


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def is_stale_timestamp(value: object, *, now: datetime, stale_after_seconds: int) -> bool:
    parsed = parse_utc_datetime(value)
    if parsed is None:
        return True
    return (now - parsed).total_seconds() >= max(1, stale_after_seconds)


def has_newer_timestamp(current: object, previous: object) -> bool:
    current_dt = parse_utc_datetime(current)
    previous_dt = parse_utc_datetime(previous)
    if current_dt is None:
        return False
    if previous_dt is None:
        return True
    return current_dt > previous_dt
