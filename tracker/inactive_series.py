from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


INTERVAL_UNITS = ("days", "weeks", "months", "years")


@dataclass(frozen=True)
class SeriesActivity:
    latest_release: datetime | None
    next_release: datetime | None
    inactive: bool


def _value(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def book_release_datetime(book: Any) -> datetime | None:
    publication = _value(book, "publication_datetime")
    raw = _value(book, "raw")
    if not publication and isinstance(raw, dict):
        publication = raw.get("publication_datetime")
    parsed = _parse_datetime(publication)
    if parsed is not None:
        return parsed

    release_date = _value(book, "release_date")
    if isinstance(release_date, str):
        return _parse_datetime(release_date[:10])
    return None


def add_interval(reference: datetime, value: int, unit: str) -> datetime:
    if value < 1:
        raise ValueError("interval value must be positive")
    if unit == "days":
        return reference + timedelta(days=value)
    if unit == "weeks":
        return reference + timedelta(weeks=value)
    if unit not in ("months", "years"):
        raise ValueError(f"unsupported interval unit: {unit}")

    months = value if unit == "months" else value * 12
    month_index = reference.year * 12 + (reference.month - 1) + months
    year, month_zero_based = divmod(month_index, 12)
    month = month_zero_based + 1
    day = min(reference.day, calendar.monthrange(year, month)[1])
    return reference.replace(year=year, month=month, day=day)


def subtract_interval(reference: datetime, value: int, unit: str) -> datetime:
    if value < 1:
        raise ValueError("interval value must be positive")
    if unit == "days":
        return reference - timedelta(days=value)
    if unit == "weeks":
        return reference - timedelta(weeks=value)
    if unit not in ("months", "years"):
        raise ValueError(f"unsupported interval unit: {unit}")

    months = value if unit == "months" else value * 12
    month_index = reference.year * 12 + (reference.month - 1) - months
    year, month_zero_based = divmod(month_index, 12)
    month = month_zero_based + 1
    day = min(reference.day, calendar.monthrange(year, month)[1])
    return reference.replace(year=year, month=month, day=day)


def classify_series_activity(
    books: list[Any] | None,
    cutoff_value: int,
    cutoff_unit: str,
    *,
    now: datetime | None = None,
) -> SeriesActivity:
    reference = now or datetime.now(timezone.utc).replace(tzinfo=None)
    if reference.tzinfo is not None:
        reference = reference.astimezone(timezone.utc).replace(tzinfo=None)

    releases: list[datetime] = []
    for book in books or []:
        if bool(_value(book, "hidden")):
            continue
        release = book_release_datetime(book)
        if release is not None:
            releases.append(release)

    past = [release for release in releases if release <= reference]
    future = [release for release in releases if release > reference]
    latest_release = max(past) if past else None
    next_release = min(future) if future else None
    boundary = subtract_interval(reference, cutoff_value, cutoff_unit)
    inactive = latest_release is not None and next_release is None and latest_release < boundary
    return SeriesActivity(latest_release, next_release, inactive)


def format_interval(value: int, unit: str) -> str:
    singular = unit[:-1] if unit.endswith("s") else unit
    return f"{value} {singular if value == 1 else unit}"


__all__ = [
    "INTERVAL_UNITS",
    "SeriesActivity",
    "add_interval",
    "book_release_datetime",
    "classify_series_activity",
    "format_interval",
    "subtract_interval",
]
