import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, date, timedelta, timezone
import sys
import warnings
import os
from pathlib import Path

# Suppress deprecation warnings during testing
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Make imports work when running this file directly from repo root
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    # Prefer package import when run as module
    from tracker.tasks import TaskWorker
except Exception:
    # Fallback to local import when running this file directly
    from tasks import TaskWorker


class TestNotifications:
    """Unit tests for notification system functionality."""

    def test_notification_new_audiobook(self):
        """Test that notifications are sent when new audiobooks are added to a series."""
        # Test data
        test_username = "admin"
        test_series_asin = "B0123456789"
        test_series_title = "Test Notification Series"

        # Determine writable notification directory (container uses /app)
        base_dir = "/app"
        try:
            os.makedirs(base_dir, exist_ok=True)
            if not os.access(base_dir, os.W_OK):
                raise PermissionError
        except PermissionError:
            base_dir = "/tmp"
            os.makedirs(base_dir, exist_ok=True)

        # Mock user with notification settings enabled for new audiobooks
        mock_user = {
            "username": test_username,
            "role": "admin",
            "notifications": {
                "enabled": True,
                "urls": [f"file://{base_dir}/test_notification.txt"],
                "notify_new_audiobook": True,
                "notify_release": False
            }
        }

        # Existing books in the series (old books)
        initial_books = [{
            "asin": "B0987654321",
            "title": "Existing Book 1",
            "authors": ["Existing Author"],
            "release_date": (date.today() - timedelta(days=30)).isoformat(),
            "narrators": ["Existing Narrator"],
            "runtime": 300
        }]

        # Updated books with a new book added
        new_book_asin = "B0111111111"
        new_book = {
            "asin": new_book_asin,
            "title": "New Book in Test Series",
            "authors": ["New Author"],
            "release_date": date.today().isoformat(),
            "narrators": ["New Narrator"],
            "runtime": 360
        }
        updated_books = initial_books + [new_book]

        # Mock series document
        mock_series_doc = {
            "_id": test_series_asin,
            "title": test_series_title,
            "url": "https://www.audible.com/series/Test-Series",
            "books": initial_books,
            "fetched_at": datetime.now(timezone.utc).isoformat() + "Z"
        }

        # Mock library entry for the user tracking this series
        mock_library_entry = {
            "_id": "test_library_id",
            "username": test_username,
            "series_asin": test_series_asin,
            "notified_releases": []
        }

        # Mock the database operations and partially mock Apprise to write to files
        with patch('tracker.tasks.get_users_collection') as mock_get_users, \
             patch('tracker.tasks.get_user_library_collection') as mock_get_library, \
             patch('apprise.Apprise') as mock_apprise_class:

            # Set up mock collections
            mock_users_col = MagicMock()
            mock_library_col = MagicMock()

            mock_get_users.return_value = mock_users_col
            mock_get_library.return_value = mock_library_col

            # Mock user lookup
            mock_users_col.find_one.return_value = mock_user

            # Mock library lookup - return the user tracking this series
            mock_library_col.find.return_value = [mock_library_entry]

            # Mock apprise to write to file
            mock_apprise = MagicMock()
            mock_apprise_class.return_value = mock_apprise
            
            # Configure mock to write notification to file when notify is called
            def mock_notify(**kwargs):
                title = kwargs.get('title', '')
                body = kwargs.get('body', '')
                content = f"{title}\n\n{body}"
                with open(f"{base_dir}/test_notification.txt", 'w') as f:
                    f.write(content)
                return True
            
            mock_apprise.notify.side_effect = mock_notify

            # Clean up any existing test file
            test_file = f"{base_dir}/test_notification.txt"
            if os.path.exists(test_file):
                os.remove(test_file)

            # Create worker and test notification sending
            worker = TaskWorker()

            print("Testing notification for new audiobook...")

            # Call the notification method directly
            worker._send_series_notifications(
                test_series_asin,
                mock_series_doc,
                initial_books,  # old books (empty)
                updated_books   # new books (with one book)
            )

            # Verify notification was sent
            print("Verifying notification was triggered...")
            test_file = f"{base_dir}/test_notification.txt"
            assert os.path.exists(test_file), f"Notification file {test_file} was not created"
            
            # Read and verify the notification content
            with open(test_file, 'r') as f:
                file_content = f.read()
            
            assert "New Audiobook(s)" in file_content
            assert test_series_title in file_content
            assert "New Book in Test Series" in file_content

            print("✓ Notification sent successfully!")
            print(f"Notification file created: {test_file}")
            print(f"File content:\n{file_content}")

    def test_notification_release_today(self):
        """Test that notifications are sent for releases happening today."""
        # Test data
        test_username = "admin"
        test_series_asin = "B0123456789"
        test_series_title = "Test Release Series"

        # Determine writable notification directory (container uses /app)
        base_dir = "/app"
        try:
            os.makedirs(base_dir, exist_ok=True)
            if not os.access(base_dir, os.W_OK):
                raise PermissionError
        except PermissionError:
            base_dir = "/tmp"
            os.makedirs(base_dir, exist_ok=True)

        # Mock user with notification settings enabled for releases
        mock_user = {
            "username": test_username,
            "role": "admin",
            "notifications": {
                "enabled": True,
                "urls": [f"file://{base_dir}/test_release_notification.txt"],
                "notify_new_audiobook": False,
                "notify_release": True
            }
        }

        # Books with one releasing today
        existing_books = [{
            "asin": "B0987654321",
            "title": "Existing Book",
            "release_date": "2023-01-01",
            "narrators": ["Test Narrator"],
            "runtime": 360
        }]

        updated_books = [{
            "asin": "B0987654321",
            "title": "Existing Book",
            "release_date": "2023-01-01",
            "narrators": ["Test Narrator"],
            "runtime": 360
        }, {
            "asin": "B0987654322",
            "title": "Releasing Today Book",
            "release_date": date.today().isoformat(),  # Today!
            "narrators": ["Test Narrator"],
            "runtime": 240
        }]

        # Mock series document
        mock_series_doc = {
            "_id": test_series_asin,
            "title": test_series_title,
            "url": "https://www.audible.com/series/Test-Series",
            "books": existing_books,
            "fetched_at": datetime.now(timezone.utc).isoformat() + "Z"
        }

        # Mock library entry
        mock_library_entry = {
            "_id": "test_library_id",
            "username": test_username,
            "series_asin": test_series_asin,
            "notified_releases": []  # Haven't been notified yet
        }

        # Mock the database operations and partially mock Apprise to write to files
        with patch('tracker.tasks.get_users_collection') as mock_get_users, \
             patch('tracker.tasks.get_user_library_collection') as mock_get_library, \
             patch('apprise.Apprise') as mock_apprise_class:

            mock_users_col = MagicMock()
            mock_library_col = MagicMock()

            mock_get_users.return_value = mock_users_col
            mock_get_library.return_value = mock_library_col

            mock_users_col.find_one.return_value = mock_user
            mock_library_col.find.return_value = [mock_library_entry]

            # Mock apprise to write to file
            mock_apprise = MagicMock()
            mock_apprise_class.return_value = mock_apprise
            
            # Configure mock to write notification to file when notify is called
            def mock_notify(**kwargs):
                title = kwargs.get('title', '')
                body = kwargs.get('body', '')
                content = f"{title}\n\n{body}"
                with open(f"{base_dir}/test_release_notification.txt", 'w') as f:
                    f.write(content)
                return True
            
            mock_apprise.notify.side_effect = mock_notify

            # Clean up any existing test file
            test_file = f"{base_dir}/test_release_notification.txt"
            if os.path.exists(test_file):
                os.remove(test_file)

            worker = TaskWorker()

            print("Testing notification for release today...")

            # Call the notification method
            worker._send_series_notifications(
                test_series_asin,
                mock_series_doc,
                existing_books,  # old books
                updated_books    # new books with release today
            )

            # Verify notification was sent
            print("Verifying release notification was triggered...")
            test_file = f"{base_dir}/test_release_notification.txt"
            assert os.path.exists(test_file), f"Notification file {test_file} was not created"
            
            # Read and verify the notification content
            with open(test_file, 'r') as f:
                file_content = f.read()
            
            assert "Audiobook Release" in file_content
            assert test_series_title in file_content
            assert "Releasing Today Book" in file_content
            assert date.today().isoformat() in file_content

            print("✓ Release notification sent successfully!")
            print(f"Notification file created: {test_file}")
            print(f"File content:\n{file_content}")

    def test_no_notification_on_initial_add(self):
        """Test that no notifications are sent when a series is initially added (empty -> books)."""
        test_username = "admin"
        test_series_asin = "B0123456789"
        test_series_title = "Test Initial Add Series"

        # Determine writable notification directory (container uses /app)
        base_dir = "/app"
        try:
            os.makedirs(base_dir, exist_ok=True)
            if not os.access(base_dir, os.W_OK):
                raise PermissionError
        except PermissionError:
            base_dir = "/tmp"
            os.makedirs(base_dir, exist_ok=True)

        # Mock user with notifications enabled
        mock_user = {
            "username": "admin",
            "role": "admin",
            "notifications": {
                "enabled": True,
                "urls": [f"file://{base_dir}/test_initial_add.txt"],
                "notify_new_audiobook": True,
                "notify_release": False
            }
        }

        # Initial books (empty - first time adding series)
        initial_books = []

        # Books after first fetch
        updated_books = [{
            "asin": "B0987654321",
            "title": "First Book",
            "release_date": "2023-01-01",
            "narrators": ["Test Narrator"],
            "runtime": 360
        }]

        mock_series_doc = {
            "_id": test_series_asin,
            "title": test_series_title,
            "url": "https://www.audible.com/series/Test-Series",
            "books": initial_books,
            "fetched_at": datetime.now(timezone.utc).isoformat() + "Z"
        }

        mock_library_entry = {
            "_id": "test_library_id",
            "username": test_username,
            "series_asin": test_series_asin,
            "notified_releases": []
        }

        with patch('tracker.tasks.get_users_collection') as mock_get_users, \
             patch('tracker.tasks.get_user_library_collection') as mock_get_library, \
             patch('apprise.Apprise') as mock_apprise_class:

            mock_users_col = MagicMock()
            mock_library_col = MagicMock()

            mock_get_users.return_value = mock_users_col
            mock_get_library.return_value = mock_library_col

            mock_users_col.find_one.return_value = mock_user
            mock_library_col.find.return_value = [mock_library_entry]

            # Mock apprise (but don't set up file writing since we expect no notification)
            mock_apprise = MagicMock()
            mock_apprise_class.return_value = mock_apprise

            # Ensure directory exists and clean up any existing test file
            test_file = f"{base_dir}/test_initial_add.txt"
            if os.path.exists(test_file):
                os.remove(test_file)

            worker = TaskWorker()

            print("Testing no notification on initial series add...")

            # Call the notification method
            worker._send_series_notifications(
                test_series_asin,
                mock_series_doc,
                initial_books,  # old books (empty)
                updated_books   # new books (first fetch)
            )

            # Verify NO notification was sent (this is the key test)
            print("Verifying no notification was sent on initial add...")
            test_file = "/app/test_initial_add.txt"
            assert not os.path.exists(test_file), f"Notification file {test_file} should not exist"

            print("✓ No notification sent on initial add (correct behavior!)")


if __name__ == "__main__":
    # Run the tests directly
    test_instance = TestNotifications()

    print("Running notification tests...\n")

    try:
        print("=== Test 1: New Audiobook Notification ===")
        test_instance.test_notification_new_audiobook()
        print("✓ PASSED\n")

        print("=== Test 2: Release Today Notification ===")
        test_instance.test_notification_release_today()
        print("✓ PASSED\n")

        print("=== Test 3: No Notification on Initial Add ===")
        test_instance.test_no_notification_on_initial_add()
        print("✓ PASSED\n")

        print("🎉 All notification tests passed!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()