from datetime import datetime

from tracker.app import _get_publication_dt
from tracker.db import get_series_collection


def test_get_publication_dt_uses_series_raw(mocker):
    # Ensure series-level publication_datetime is used when book lacks it
    series_col = get_series_collection()
    series_col.delete_many({})
    series_col.insert_one({
        "_id": "S1",
        "raw": {"publication_datetime": "2026-01-06T08:00:00Z"},
    })

    book = {
        "asin": "B1",
        "release_date": "2026-01-06",
        # no publication_datetime on the book
    }

    dt = _get_publication_dt(book, series_asin="S1", series_cache={})
    assert isinstance(dt, datetime)
    assert dt == datetime(2026, 1, 6, 8)


def test_get_publication_dt_prefers_book_raw(mocker):
    series_col = get_series_collection()
    series_col.delete_many({})
    series_col.insert_one({
        "_id": "S1",
        "raw": {"publication_datetime": "2026-01-06T08:00:00Z"},
        "books": [
            {"asin": "B2", "raw": {"publication_datetime": "2026-01-06T05:00:00Z"}}
        ],
    })

    book = {"asin": "B2", "release_date": "2026-01-06"}
    dt = _get_publication_dt(book, series_asin="S1", series_cache={})
    assert dt == datetime(2026, 1, 6, 5)


def test_format_time_left_hours():
    from tracker.app import _format_time_left
    now = datetime(2026,1,5,20,0,0)
    release = datetime(2026,1,6,2,30,0)  # 6.5 hours later
    s, hours, days = _format_time_left(release, now)
    assert s == "7 hours"
    assert hours == 7
    assert days is None


def test_format_time_left_days():
    from tracker.app import _format_time_left
    now = datetime(2026,1,1,0,0,0)
    release = datetime(2026,1,3,10,0,0)  # ~2.416 days => 3 days when rounded up
    s, hours, days = _format_time_left(release, now)
    assert s == "3 days"
    assert hours is None
    assert days == 3
