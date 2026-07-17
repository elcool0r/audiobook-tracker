from datetime import datetime, timezone

import pytest

from tracker.inactive_series import (
    add_interval,
    book_release_datetime,
    classify_series_activity,
    subtract_interval,
)


NOW = datetime(2026, 7, 14, 12, 0, 0)


def test_old_release_is_inactive():
    activity = classify_series_activity([{"release_date": "2023-01-01"}], 2, "years", now=NOW)
    assert activity.inactive is True
    assert activity.latest_release == datetime(2023, 1, 1)
    assert activity.next_release is None


def test_release_exactly_on_cutoff_is_active():
    activity = classify_series_activity([{"publication_datetime": "2024-07-14T12:00:00"}], 2, "years", now=NOW)
    assert activity.inactive is False


def test_future_release_keeps_series_active():
    books = [
        {"release_date": "2020-01-01"},
        {"publication_datetime": "2027-01-01T08:00:00Z"},
    ]
    activity = classify_series_activity(books, 2, "years", now=NOW)
    assert activity.inactive is False
    assert activity.next_release == datetime(2027, 1, 1, 8, 0, 0)


@pytest.mark.parametrize("books", [[], [{"release_date": "invalid"}], [{"title": "Undated"}]])
def test_missing_or_invalid_dates_stay_active(books):
    assert classify_series_activity(books, 2, "years", now=NOW).inactive is False


def test_hidden_books_do_not_affect_activity():
    books = [
        {"release_date": "2020-01-01"},
        {"release_date": "2027-01-01", "hidden": True},
    ]
    assert classify_series_activity(books, 2, "years", now=NOW).inactive is True


def test_publication_datetime_takes_precedence_and_normalizes_timezone():
    book = {
        "publication_datetime": "2024-07-14T14:00:00+02:00",
        "release_date": "2020-01-01",
    }
    assert book_release_datetime(book) == datetime(2024, 7, 14, 12, 0, 0)


def test_raw_publication_datetime_falls_back_before_release_date():
    book = {
        "raw": {"publication_datetime": "2024-07-14T12:00:00Z"},
        "release_date": "2020-01-01",
    }
    assert book_release_datetime(book) == datetime(2024, 7, 14, 12, 0, 0)


def test_calendar_month_clamps_month_end():
    assert add_interval(datetime(2024, 1, 31), 1, "months") == datetime(2024, 2, 29)
    assert subtract_interval(datetime(2024, 3, 31), 1, "months") == datetime(2024, 2, 29)


def test_calendar_year_clamps_leap_day():
    assert add_interval(datetime(2024, 2, 29), 1, "years") == datetime(2025, 2, 28)
    assert subtract_interval(datetime(2024, 2, 29), 1, "years") == datetime(2023, 2, 28)


def test_day_and_week_intervals_preserve_timezone():
    reference = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    assert add_interval(reference, 2, "weeks") == datetime(2026, 7, 28, 12, tzinfo=timezone.utc)
    assert subtract_interval(reference, 3, "days") == datetime(2026, 7, 11, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("value,unit", [(0, "days"), (1, "hours")])
def test_invalid_intervals_are_rejected(value, unit):
    with pytest.raises(ValueError):
        add_interval(NOW, value, unit)
