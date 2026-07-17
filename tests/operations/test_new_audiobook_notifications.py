import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tracker.tasks import TaskWorker


def test_periodic_new_audiobook_notification_uses_initialized_state():
    library_entry = {
        "_id": "library-entry-1",
        "username": "daniel",
        "series_asin": "SERIES-1",
        "notified_new_asins": ["BOOK-1"],
        "notified_new_asins_initialized": True,
        "notified_releases": [],
    }

    library_collection = MagicMock()
    library_collection.count_documents.return_value = 1
    library_collection.distinct.side_effect = lambda field, _filter: {
        "username": ["daniel"],
        "series_asin": ["SERIES-1"],
    }[field]

    def find_library_entries(_filter, projection):
        projected_entry = {
            key: value
            for key, value in library_entry.items()
            if projection.get(key)
        }
        cursor = MagicMock()
        cursor.batch_size.return_value = [projected_entry]
        return cursor

    library_collection.find.side_effect = find_library_entries

    users_collection = MagicMock()
    users_collection.find.return_value = [
        {
            "username": "daniel",
            "notifications": {
                "enabled": True,
                "notify_new_audiobook": True,
                "urls": ["ntfy://example"],
            },
        }
    ]

    series_collection = MagicMock()
    series_collection.find.return_value = [
        {
            "_id": "SERIES-1",
            "title": "Test Series",
            "books": [
                {"asin": "BOOK-1", "title": "Existing Book"},
                {"asin": "BOOK-2", "title": "New Book"},
            ],
        }
    ]

    apprise_instance = MagicMock()
    apprise_instance.notify.return_value = True
    apprise_module = SimpleNamespace(Apprise=MagicMock(return_value=apprise_instance))
    worker = TaskWorker()

    with (
        patch("tracker.tasks.get_user_library_collection", return_value=library_collection),
        patch("tracker.tasks.get_users_collection", return_value=users_collection),
        patch("tracker.tasks.get_series_collection", return_value=series_collection),
        patch("tracker.tasks.compute_narrator_warnings", return_value=[]),
        patch.object(worker, "_record_notification_job"),
        patch.dict(sys.modules, {"apprise": apprise_module}),
    ):
        worker._check_new_audiobook_notifications()

    apprise_instance.notify.assert_called_once_with(
        title="New Audiobook(s)",
        body="New audiobooks found in 'Test Series':\n- New Book",
    )
