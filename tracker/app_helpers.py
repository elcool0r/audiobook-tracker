from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from .db import get_series_collection


def compute_num_latest(user_doc: dict | None) -> int:
    try:
        num_latest = int((user_doc or {}).get('latest_count') or 4)
    except Exception:
        num_latest = 4
    return max(1, min(24, num_latest))


def parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat((s or "").split("T")[0]).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_date_naive(s: str | None) -> datetime | None:
    """Parse date string (YYYY-MM-DD or ISO datetime) and return a naive datetime (no tzinfo)."""
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.split("T")[0])
    except Exception:
        return None


def format_dt(dt: datetime | None, date_format: str = "de") -> str:
    if not dt:
        return "—"
    def pad(n):
        return str(n).zfill(2)
    if date_format == "de":
        return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
    if date_format == "us":
        return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
    return f"{dt.date().isoformat()} {pad(dt.hour)}:{pad(dt.minute)}"


def format_d(dt: datetime | None, date_format: str = "de") -> str:
    if not dt:
        return "—"
    def pad(n):
        return str(n).zfill(2)
    if date_format == "de":
        return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year}"
    if date_format == "us":
        return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year}"
    return dt.date().isoformat()


def format_runtime(val: Any) -> str | None:
    try:
        m = int(val or 0)
    except Exception:
        return None
    if m <= 0:
        return None
    h = m // 60
    mins = m % 60
    return f"{h}h {mins}m" if h else f"{mins}m"


def preload_series_data(series_asins: List[str]) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    series_cache: Dict[str, Any] = {}
    narrator_warnings_map: Dict[str, List[str]] = {}
    if not series_asins:
        return series_cache, narrator_warnings_map
    try:
        docs = get_series_collection().find(
            {"_id": {"$in": series_asins}},
            {"books": 1, "publication_datetime": 1, "raw.publication_datetime": 1, "narrator_warnings": 1}
        )
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            series_cache[doc.get("_id")] = doc
            narrator_warnings_map[doc.get("_id")] = doc.get("narrator_warnings", []) or []
    except Exception:
        return {}, {}
    return series_cache, narrator_warnings_map


def format_time_left(release_dt: datetime, now: datetime) -> tuple[str, int | None, int | None]:
    """Return a (time_left_str, hours_left or None, days_left or None).

    Mirrors the previous behavior in `tracker.app._format_time_left`.
    """
    try:
        delta = release_dt - now
        total_seconds = delta.total_seconds()
    except Exception:
        return ("today", None, 0)
    if total_seconds <= 0:
        return ("today", None, 0)
    one_day = 24 * 60 * 60
    if total_seconds < one_day:
        hours = int((total_seconds + 3599) // 3600)
        return (f"{hours} hours", hours, None)
    days = int((total_seconds + one_day - 1) // one_day)
    return (f"{days} days", None, days)


__all__ = [
    "compute_num_latest",
    "parse_date",
    "parse_date_naive",
    "format_dt",
    "format_d",
    "format_runtime",
    "preload_series_data",
    "format_time_left",
]
