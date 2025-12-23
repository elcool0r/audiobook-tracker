import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure repo import paths work when running tests from project root
ROOT = Path(__file__).resolve().parent

from tracker.tasks import TaskWorker


def _get_base_dir():
    base_dir = "/app"
    try:
        os.makedirs(base_dir, exist_ok=True)
        if not os.access(base_dir, os.W_OK):
            raise PermissionError
    except PermissionError:
        base_dir = "/tmp"
        os.makedirs(base_dir, exist_ok=True)
    return base_dir


class TestReleaseFlow:
    def test_new_book_then_release_after_one_minute(self):
        """Flow: add a series tracked by admin, add a new book -> new-audiobook notification,
        then simulate 1 minute later and verify release notification is sent. Clean up files afterwards."""
        base_dir = _get_base_dir()

        # Use timezone-aware UTC datetimes
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        pub_dt = (now + timedelta(seconds=60)).replace(microsecond=0)
        pub_iso = pub_dt.isoformat() + "Z"

        test_username = "admin"
        test_series_asin = "B0FLOWTEST1"
        test_series_title = "Flow Test Series"

        # Initial books (existing)
        initial_books = [{
            "asin": "B0OLD000001",
            "title": "Old Book",
            "release_date": (now - timedelta(days=10)).date().isoformat(),
            "narrators": ["Narrator A"],
            "runtime": 300
        }]

        # New book being added - publication datetime in 1 minute
        new_book_asin = "B0NEW000002"
        new_book = {
            "asin": new_book_asin,
            "title": "Soon To Release Book",
            "authors": ["Author X"],
            "publication_datetime": pub_iso,
            "narrators": ["Narrator X"],
            "runtime": 360,
            "image": "https://example.com/soon_cover.jpg"
        }

        updated_books = initial_books + [new_book]

        # Series docs
        initial_series_doc = {
            "_id": test_series_asin,
            "title": test_series_title,
            "url": "https://www.audible.com/series/Flow-Test",
            "books": initial_books,
            "fetched_at": datetime.now(timezone.utc).isoformat() + "Z"
        }

        series_doc_with_new = dict(initial_series_doc)
        series_doc_with_new["books"] = updated_books

        # Library entry for user tracking this series
        library_entry = {
            "_id": "lib_flow_1",
            "username": test_username,
            "series_asin": test_series_asin,
            "notified_releases": [],
            "notified_new_asins": []
        }

        # User with both notifications enabled
        mock_user = {
            "username": test_username,
            "role": "admin",
            "notifications": {
                "enabled": True,
                "urls": [f"file://{base_dir}/flow_new.txt", f"file://{base_dir}/flow_release.txt"],
                "notify_new_audiobook": True,
                "notify_release": True,
            }
        }

        # Ensure files are cleaned up before starting
        new_file = f"{base_dir}/flow_new.txt"
        release_file = f"{base_dir}/flow_release.txt"
        for p in (new_file, release_file):
            if os.path.exists(p):
                os.remove(p)

        with patch('tracker.tasks.get_users_collection') as mock_get_users, \
             patch('tracker.tasks.get_user_library_collection') as mock_get_library, \
             patch('tracker.tasks.get_series_collection') as mock_get_series, \
             patch('apprise.Apprise') as mock_apprise_class:

            mock_users_col = MagicMock()
            mock_library_col = MagicMock()
            mock_series_col = MagicMock()

            mock_get_users.return_value = mock_users_col
            mock_get_library.return_value = mock_library_col
            mock_get_series.return_value = mock_series_col

            mock_users_col.find_one.return_value = mock_user
            mock_library_col.find.return_value = [library_entry]

            # series_col.find_one should return the updated series when asked
            mock_series_col.find_one.return_value = series_doc_with_new

            # Mock apprise to write to files depending on title
            mock_apprise = MagicMock()
            mock_apprise_class.return_value = mock_apprise

            def mock_notify(**kwargs):
                title = kwargs.get('title', '')
                body = kwargs.get('body', '')
                attach = kwargs.get('attach', None)
                if 'New Audiobook' in title:
                    fpath = new_file
                elif 'Audiobook Release' in title:
                    fpath = release_file
                else:
                    fpath = f"{base_dir}/flow_other.txt"
                with open(fpath, 'w') as f:
                    f.write(f"{title}\n\n{body}\n\nATTACH: {attach}")
                return True

            mock_apprise.notify.side_effect = mock_notify

            worker = TaskWorker()

            # Step 1: Simulate adding the new book (trigger new-audiobook notification)
            worker._send_series_notifications(
                test_series_asin,
                series_doc_with_new,
                initial_books,
                updated_books,
            )

            # Verify the new audiobook notification was written
            assert os.path.exists(new_file), "New audiobook notification file was not created"
            with open(new_file, 'r') as fh:
                content = fh.read()
            assert "New Audiobook" in content
            assert test_series_title in content
            assert new_book['title'] in content
            assert new_book['image'] in content

            # Verify that notified_new_asins were persisted for the library entry
            # (update_one should be called to add to notified_new_asins)
            mock_library_col.update_one.assert_any_call(
                {"_id": library_entry.get("_id")},
                {"$addToSet": {"notified_new_asins": {"$each": [new_book_asin]}}}
            )

            # Step 2: Simulate time advancing by 61 seconds and run release notifier
            simulated_now = pub_dt + timedelta(seconds=1)
            with patch('tracker.tasks._now_dt', return_value=simulated_now):
                # series_col.find_one already returns series_doc_with_new
                worker._check_due_release_notifications()

            # Verify the release notification was written
            assert os.path.exists(release_file), "Release notification file was not created"
            with open(release_file, 'r') as fh:
                rcontent = fh.read()
            assert "Audiobook Release" in rcontent
            assert new_book['title'] in rcontent

            # Verify that notified_releases were persisted for the library entry
            mock_library_col.update_one.assert_any_call(
                {"_id": library_entry.get("_id")},
                {"$addToSet": {"notified_releases": {"$each": [new_book_asin]}}}
            )

        # Cleanup created files
        for p in (new_file, release_file):
            if os.path.exists(p):
                os.remove(p)

    def test_background_new_audiobook_notifications(self):
        base_dir = _get_base_dir()

        test_username = "admin"
        test_series_asin = "B0FLOWTESTX"
        test_series_title = "Background Flow"

        initial_book = {
            "asin": "B0BGINITIAL",
            "title": "Initial Book",
            "release_date": "2023-01-01",
        }

        new_book = {
            "asin": "B0BGSPOIL",
            "title": "Background Splash",
            "publication_datetime": "2025-12-24T00:00:00Z",
            "image": "https://example.com/bg_cover.jpg",
        }

        initial_series_doc = {
            "_id": test_series_asin,
            "title": test_series_title,
            "books": [initial_book],
            "fetched_at": datetime.now(timezone.utc).isoformat() + "Z",
        }

        series_with_new = dict(initial_series_doc)
        series_with_new["books"] = [initial_book, new_book]

        library_entry = {
            "_id": "lib_flow_bg",
            "username": test_username,
            "series_asin": test_series_asin,
            "notified_new_asins": [],
        }

        mock_user = {
            "username": test_username,
            "role": "admin",
            "notifications": {
                "enabled": True,
                "urls": [f"file://{base_dir}/bg_new.txt"],
                "notify_new_audiobook": True,
                "notify_release": False,
            },
        }

        new_file = f"{base_dir}/bg_new.txt"
        if os.path.exists(new_file):
            os.remove(new_file)

        with patch('tracker.tasks.get_users_collection') as mock_get_users, \
             patch('tracker.tasks.get_user_library_collection') as mock_get_library, \
             patch('tracker.tasks.get_series_collection') as mock_get_series, \
             patch('apprise.Apprise') as mock_apprise_class:

            mock_users_col = MagicMock()
            mock_library_col = MagicMock()
            mock_series_col = MagicMock()

            mock_get_users.return_value = mock_users_col
            mock_get_library.return_value = mock_library_col
            mock_get_series.return_value = mock_series_col

            mock_users_col.find_one.return_value = mock_user
            mock_library_col.find.return_value = [library_entry]
            mock_series_col.find_one.return_value = initial_series_doc

            mock_apprise = MagicMock()
            mock_apprise_class.return_value = mock_apprise

            def mock_notify(**kwargs):
                with open(new_file, 'w') as fh:
                    fh.write(kwargs.get('body', ''))
                return True

            mock_apprise.notify.side_effect = mock_notify

            worker = TaskWorker()

            worker._check_new_audiobook_notifications()
            assert not mock_apprise.notify.called, "Initial sync should not trigger a notification"
            assert mock_library_col.update_one.call_count >= 1

            library_entry["notified_new_asins"] = [initial_book["asin"]]
            library_entry["notified_new_asins_initialized"] = True
            mock_series_col.find_one.return_value = series_with_new

            worker._check_new_audiobook_notifications()
            assert os.path.exists(new_file), "Background new-audiobook notification file missing"
            with open(new_file, 'r') as fh:
                body = fh.read()
            assert new_book["title"] in body

        if os.path.exists(new_file):
            os.remove(new_file)
