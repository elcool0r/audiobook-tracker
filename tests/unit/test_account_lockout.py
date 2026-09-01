"""Account lockout must expire rather than latch permanently."""
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from unittest.mock import patch

from tracker.auth import (
    MAX_FAILED_ATTEMPTS,
    is_account_locked,
    record_failed_attempt,
    record_successful_login,
)


@pytest.fixture
def users():
    col = mongomock.MongoClient().db.users
    col.insert_one({"username": "u", "failed_attempts": 0})
    with patch("tracker.auth.get_users_collection", return_value=col):
        yield col


def _doc(users):
    return users.find_one({"username": "u"})


def test_lock_engages_at_the_threshold(users):
    for _ in range(MAX_FAILED_ATTEMPTS - 1):
        record_failed_attempt("u")
    assert not is_account_locked(_doc(users))
    record_failed_attempt("u")
    assert is_account_locked(_doc(users))


def test_counter_resets_after_the_lock_expires(users):
    """An expired lock previously left failed_attempts latched at the threshold,
    so the very next attempt re-locked the account indefinitely."""
    for _ in range(MAX_FAILED_ATTEMPTS):
        record_failed_attempt("u")
    # Simulate the lock window elapsing.
    users.update_one({"username": "u"},
                     {"$set": {"lock_until": datetime.now(timezone.utc) - timedelta(minutes=1)}})
    assert not is_account_locked(_doc(users))

    record_failed_attempt("u")
    assert _doc(users)["failed_attempts"] == 1
    assert not is_account_locked(_doc(users)), "one failed attempt must not re-lock"


def test_successful_login_clears_state(users):
    for _ in range(MAX_FAILED_ATTEMPTS):
        record_failed_attempt("u")
    record_successful_login("u")
    assert _doc(users)["failed_attempts"] == 0
    assert not is_account_locked(_doc(users))
