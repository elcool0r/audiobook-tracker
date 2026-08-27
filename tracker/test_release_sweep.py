import os
from datetime import datetime, timedelta, timezone

import pytest

from tracker.db import get_db
from tracker.tasks import worker


@pytest.fixture
def db(monkeypatch):
    """A fresh in-memory database.

    MONGO_URI has to be cleared inside the test rather than at import time: other
    test modules set it from their fixtures, so an import-time pop is undone by
    whatever ran before this module.
    """
    monkeypatch.delenv("MONGO_URI", raising=False)
    monkeypatch.setenv("ALLOW_IN_MEMORY_DB", "1")
    get_db.cache_clear()
    try:
        yield get_db()
    finally:
        get_db.cache_clear()


def test_check_due_release_notifications_picks_up_raw_pubdt(db):
    # Clean collections
    db.users.delete_many({})
    db.user_library.delete_many({})
    db.series.delete_many({})
    db.jobs.delete_many({})

    # Create user with notifications enabled
    db.users.insert_one({
        "username": "testuser",
        "notifications": {"enabled": True, "notify_release": True, "urls": ["ntfy://example"]},
    })

    # Create library entry tracking series
    db.user_library.insert_one({
        "username": "testuser",
        "series_asin": "S1",
        "notified_releases": [],
    })

    # Create series with one book that has raw.publication_datetime set to now - 2 minutes
    now = datetime.now(timezone.utc).replace(microsecond=0)
    pub = (now - timedelta(minutes=2)).isoformat().replace('+00:00', 'Z')
    db.series.insert_one({
        "_id": "S1",
        "title": "Test Series",
        "books": [
            {"asin": "B1", "title": "Book 1", "raw": {"publication_datetime": pub}},
        ],
    })

    # Run sweep
    worker._check_due_release_notifications()

    # Assert a release_notification job was created
    jobs = list(db.jobs.find({"type": "release_notification"}))
    assert len(jobs) == 1, f"Expected 1 release job, found {len(jobs)}"
    job = jobs[0]
    assert "B1" in job.get("result", {}).get("notified_asins", []), job

    # Assert user_library was updated marking the ASIN
    entry = db.user_library.find_one({"username": "testuser", "series_asin": "S1"})
    assert entry is not None
    assert "B1" in (entry.get("notified_releases") or []), entry
