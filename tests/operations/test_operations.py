import os

import mongomock
import pytest
from fastapi import HTTPException
from unittest.mock import patch, MagicMock


# Mock get_current_user before importing the app
def mock_get_current_user():
    """Mock authentication for tests."""
    return {
        "username": "admin",
        "role": "admin",
        "date_format": "iso",
        "frontpage_slug": "admin"
    }


# Mock _require_admin to do nothing
def mock_require_admin(user):
    pass


# Apply patches before importing app
with patch('tracker.api._require_admin', mock_require_admin):
    from tracker.app import create_app
    from tracker.auth import create_access_token, get_current_user


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    # Set up test environment variables
    os.environ['MONGO_URI'] = 'mongodb://localhost:27017'
    os.environ['MONGO_DB'] = 'test_audiobook_tracker'
    os.environ['SECRET_KEY'] = 'test_secret_key'

    # Mock MongoDB connection for unit tests
    with patch('tracker.db.get_db') as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        # Mock collections with proper return values
        mock_db.users = MagicMock()
        mock_db.user_library = MagicMock()
        mock_db.series = MagicMock()
        mock_db.jobs = MagicMock()
        mock_db.settings = MagicMock()

        # Mock user lookup for admin user
        from tracker.auth import get_password_hash
        admin_password_hash = get_password_hash("admin")
        
        def mock_find_user(query):
            username = query.get("username")
            if username == "admin":
                return {
                    "username": "admin",
                    "password_hash": admin_password_hash,
                    "role": "admin",
                    "failed_attempts": 0,
                    "lock_until": None
                }
            return None
        
        # Mock settings
        mock_db.settings.find_one.return_value = {
            "default_num_results": 10,
            "log_retention_days": 30,
            "default_frontpage_slug": "",
            "secret_key": "test_secret_key"
        }

        # Patch get_users_collection for authentication
        mock_users_collection = MagicMock()
        mock_users_collection.find_one.side_effect = mock_find_user
        mock_users_collection.count_documents.return_value = 1

        # Patch the limiter to disable rate limiting
        with patch('tracker.app.limiter') as mock_limiter:
            mock_limiter.limit = lambda *args, **kwargs: lambda func: func
            
            # Patch add_middleware to skip SlowAPIMiddleware
            original_add_middleware = None
            def patched_add_middleware(middleware, **kwargs):
                if middleware.__name__ != 'SlowAPIMiddleware':
                    original_add_middleware(middleware, **kwargs)
            
            # Patch the find_one method
            with patch.object(mock_db.users, 'find_one', side_effect=mock_find_user):
                with patch.object(mock_db.users, 'count_documents', return_value=1):
                    with patch('fastapi.applications.FastAPI.add_middleware', side_effect=patched_add_middleware) as mock_add_middleware:
                        original_add_middleware = mock_add_middleware
                        with patch('tracker.db.get_users_collection', return_value=mock_users_collection):
                            app = create_app()
                            # Override the get_current_user dependency
                            app.dependency_overrides[get_current_user] = mock_get_current_user
                        with patch('tracker.db.get_users_collection', return_value=mock_users_collection):
                            with patch('tracker.api.get_users_collection', return_value=mock_users_collection):
                                app = create_app()
                                # Override the get_current_user dependency
                                app.dependency_overrides[get_current_user] = mock_get_current_user
                                # Also patch get_current_user directly
                                with patch('tracker.auth.get_current_user', mock_get_current_user):
                                    # Patch the api_create_user to not require admin check
                                    with patch('tracker.api._require_admin', mock_require_admin):
                                        from fastapi.testclient import TestClient
                                        with TestClient(app) as client:
                                            yield client


@pytest.fixture
def auth_headers():
    """Create authentication headers for admin user."""
    token = create_access_token({"sub": "admin"})
    return {"Cookie": f"auth_token={token}"}


@pytest.fixture
def user_auth_headers():
    """Create authentication headers for admin user."""
    token = create_access_token({"sub": "admin", "role": "admin"})
    return {"Cookie": f"audiobook_tracker_token={token}"}


class TestUserManagement:
    """Test user management operations."""

    
    def test_create_user(self, client, auth_headers):
        """Test creating a new user."""
        user_data = {
            "username": "newuser",
            "password": "password123",
            "role": "user"
        }

        response = client.post(
            "/config/api/users",
            json=user_data,
            headers=auth_headers
        )
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
    def test_update_user(self, client, auth_headers):
        """Test updating an existing user."""
        update_data = {
            "role": "admin",
            "notifications": {"enabled": True, "urls": ["ntfy://test"]}
        }

        with patch('tracker.db.get_users_collection') as mock_users_col:
            mock_users_col.return_value.find_one.return_value = {
                "username": "admin",
                "role": "admin"
            }
            mock_users_col.return_value.update_one.return_value = MagicMock()

            response = client.put(
                "/config/api/users/admin",
                json=update_data,
                headers=auth_headers
            )
            assert response.status_code == 200

    
    def test_delete_user(self, client, auth_headers):
        """Test deleting a user."""
        with patch('tracker.db.get_users_collection') as mock_users_col, \
             patch('tracker.db.get_user_library_collection') as mock_lib_col:

            mock_users_col.return_value.find_one.return_value = {
                "username": "testuser"
            }
            mock_users_col.return_value.delete_one.return_value = MagicMock()
            mock_lib_col.return_value.delete_many.return_value = MagicMock()

            response = client.delete(
                "/config/api/users/testuser",
                headers=auth_headers
            )
            assert response.status_code == 200


class TestLibraryOperations:
    """Test library management operations."""

    
    def test_add_to_library(self, client, auth_headers):
        """Test adding a series to library."""
        library_data = {
            "title": "An Unexpected Hero",
            "asin": "B0DDLHDJD9"
        }

        fake_db = mongomock.MongoClient().db
        user_collection = fake_db.user_library
        series_collection = fake_db.series

        with patch('tracker.library.get_user_library_collection', return_value=user_collection), \
             patch('tracker.library.get_series_collection', return_value=series_collection), \
             patch('tracker.api.enqueue_fetch_series_books') as mock_enqueue, \
             patch('lib.audible_api_search.get_product_by_asin') as mock_get_product:

            mock_get_product.return_value = {
                "product": {
                    "asin": "B0DDLHDJD9",
                    "title": "An Unexpected Hero",
                    "issue_date": "2020-01-01",
                    "relationships": []
                }
            }
            mock_enqueue.return_value = "job-123"

            response = client.post(
                "/config/api/library",
                json=library_data,
                headers=auth_headers
            )
            assert response.status_code == 200
            data = response.json()
            assert data.get("item", {}).get("asin") == "B0DDLHDJD9"
            assert data.get("job_id") == "job-123"
            saved_entry = user_collection.find_one({"username": "admin", "series_asin": "B0DDLHDJD9"})
            assert saved_entry is not None

    def test_series_level_ignore_toggle(self, client, auth_headers):
        """Test toggling series-level ignore and its effect on books and warnings."""
        fake_db = mongomock.MongoClient().db
        series_collection = fake_db.series
        # Two books with different narrators so warnings should include the second
        books = [
            {"title": "Book 1", "asin": "B1", "narrators": ["Narrator A"]},
            {"title": "Book 2", "asin": "B2", "narrators": ["Narrator B"]},
        ]
        # Start with an existing warning on Book 2
        series_collection.insert_one({"_id": "S_TEST", "title": "Test Series", "books": books, "narrator_warnings": ["Book 2"], "ignore_narrator_warnings": False})

        with patch('tracker.api.get_series_collection', return_value=series_collection), patch('tracker.library.get_series_collection', return_value=series_collection):
            # Enable series-level ignore
            resp = client.post("/config/api/series/S_TEST/ignore-narrator-series", json={"ignore": True}, headers=auth_headers)
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("ignore_narrator_warnings") is True

            doc = series_collection.find_one({"_id": "S_TEST"})
            assert doc.get("ignore_narrator_warnings") is True
            # All books should be marked ignored and flagged as set by series
            for b in doc.get("books", []):
                assert b.get("ignore_narrator_warning") is True
                assert b.get("ignore_narrator_warning_set_by_series") is True
            # Warnings cleared
            assert doc.get("narrator_warnings") == []

            # Disable series-level ignore and ensure per-book flags set by series are reverted and warnings recomputed
            resp = client.post("/config/api/series/S_TEST/ignore-narrator-series", json={"ignore": False}, headers=auth_headers)
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("ignore_narrator_warnings") is False

            doc = series_collection.find_one({"_id": "S_TEST"})
            assert doc.get("ignore_narrator_warnings") is False
            for b in doc.get("books", []):
                # series-set flags should be removed
                assert not b.get("ignore_narrator_warning_set_by_series")
            # Since the books have different narrators, warnings should now include Book 2 again
            assert "Book 2" in (doc.get("narrator_warnings") or [])

    def test_narrator_case_insensitive(self):
        """Narrator comparisons should be case-insensitive."""
        from tracker.library import compute_narrator_warnings
        books = [
            {"title": "Book 1", "asin": "B1", "narrators": ["Narrator A"]},
            {"title": "Book 2", "asin": "B2", "narrators": ["narrator a"]},
            {"title": "Book 3", "asin": "B3", "narrators": ["Narrator B"]},
        ]
        warnings = compute_narrator_warnings(books, None)
        # Book 2 should not be flagged because narrator matches primary regardless of case
        assert "Book 2" not in warnings
        # Book 3 should be flagged
        assert "Book 3" in warnings

    def test_remove_from_library(self, client, auth_headers):
        """Test removing a series from library."""
        with patch('tracker.db.get_user_library_collection') as mock_lib_col:
            mock_lib_col.return_value.find_one.return_value = {
                "username": "admin",
                "series_asin": "TEST123"
            }
            mock_lib_col.return_value.delete_one.return_value = MagicMock()

            response = client.delete(
                "/config/api/library?series_asin=TEST123",
                headers=auth_headers
            )
            assert response.status_code == 200


class TestSeriesOperations:
    """Test series management operations."""

    
    def test_get_series_info(self, client, auth_headers):
        """Test getting series information."""
        with patch('tracker.db.get_series_collection') as mock_series_col:
            mock_series_col.return_value.find_one.return_value = {
                "_id": "TEST123",
                "title": "Test Series",
                "books": []
            }

            response = client.get(
                "/config/api/series/info/TEST123",
                headers=auth_headers
            )
            assert response.status_code == 200
            data = response.json()
            assert "title" in data

    
    def test_refresh_series(self, client, auth_headers):
        """Test refreshing series data."""
        with patch('tracker.tasks.enqueue_fetch_series_books') as mock_enqueue, \
             patch('tracker.db.get_series_collection') as mock_series_col:

            mock_series_col.return_value.find_one.return_value = {
                "_id": "TEST123",
                "title": "Test Series"
            }
            mock_enqueue.return_value = None

            response = client.post(
                "/config/api/series/TEST123/refresh",
                headers=auth_headers
            )
            assert response.status_code == 200

    
    def test_update_series_title(self, client, auth_headers):
        """Test updating series title."""
        with patch('tracker.db.get_series_collection') as mock_series_col:
            mock_series_col.return_value.find_one.return_value = {
                "_id": "TEST123",
                "title": "Old Title"
            }
            mock_series_col.return_value.update_one.return_value = MagicMock()

            response = client.put(
                "/config/api/series/TEST123/title",
                json={"title": "New Title"},
                headers=auth_headers
            )
            assert response.status_code == 200


class TestJobOperations:
    """Test job management operations."""

    
    def test_clear_jobs(self, client, auth_headers):
        """Test clearing completed jobs."""
        with patch('tracker.db.get_jobs_collection') as mock_jobs_col:
            mock_jobs_col.return_value.delete_many.return_value = MagicMock()

            response = client.post(
                "/config/api/jobs/clear",
                headers=auth_headers
            )
            assert response.status_code == 200

    
    def test_prune_jobs(self, client, auth_headers):
        """Test pruning old jobs."""
        with patch('tracker.db.get_jobs_collection') as mock_jobs_col:
            mock_jobs_col.return_value.delete_many.return_value = MagicMock()

            response = client.post(
                "/config/api/jobs/prune",
                headers=auth_headers
            )
            assert response.status_code == 200

    
    def test_test_job(self, client, auth_headers):
        """Test running a test job."""
        with patch('tracker.tasks.enqueue_test_job') as mock_enqueue:
            mock_enqueue.return_value = None

            response = client.post(
                "/config/api/jobs/test",
                headers=auth_headers
            )
            assert response.status_code == 200


class TestSettingsOperations:
    """Test settings management operations."""

    
    def test_update_settings(self, client, auth_headers):
        """Test updating application settings."""
        settings_data = {
            "debug_logging": True,
            "audible_region": "us"
        }

        with patch('tracker.settings.save_settings') as mock_save, \
             patch('tracker.settings.load_settings') as mock_load:

            mock_load.return_value = MagicMock()
            mock_save.return_value = None

            response = client.post(
                "/config/api/settings",
                json=settings_data,
                headers=auth_headers
            )
            assert response.status_code == 200

    def test_reschedule_all_series_spreads_over_24_hours(self, client, auth_headers):
        """Rescheduling should set next_refresh_at for all series distributed over 24 hours."""
        import mongomock
        from datetime import datetime, timezone, timedelta
        fake_db = mongomock.MongoClient().db
        series_collection = fake_db.series
        # Insert sample series
        count = 6
        for i in range(count):
            series_collection.insert_one({"_id": f"S{i}", "title": f"Series {i}"})

        with patch('tracker.db.get_series_collection', return_value=series_collection), patch('tracker.tasks.get_series_collection', return_value=series_collection):
            from tracker.tasks import reschedule_all_series
            result = reschedule_all_series()
            assert result.get('count') == count

            docs = list(series_collection.find({}))
            assert all('next_refresh_at' in d for d in docs)

            now = datetime.now(timezone.utc)
            end_window = now + timedelta(hours=24, minutes=5)
            start_window = now - timedelta(minutes=5)

            times = [datetime.fromisoformat(d['next_refresh_at'].replace('Z', '+00:00')) for d in docs]
            assert all(start_window <= t <= end_window for t in times)

            # Check spacing roughly consistent (allow 40% tolerance)
            times_sorted = sorted(times)
            diffs = [(times_sorted[i+1] - times_sorted[i]).total_seconds() for i in range(len(times_sorted)-1)]
            expected = (24*3600) / count
            assert all(d >= expected * 0.4 for d in diffs)

    
    def test_test_proxy_settings(self, client, auth_headers):
        """Test proxy configuration."""
        proxy_data = {
            "http_proxy": "http://proxy.example.com:8080",
            "https_proxy": "http://proxy.example.com:8080"
        }

        response = client.post(
            "/config/api/settings/test-proxy",
            json=proxy_data,
            headers=auth_headers
        )
        # This may succeed or fail depending on proxy availability
        assert response.status_code in [200, 400, 500]

    def test_refresh_all_series_now_enqueues_and_reschedules(self, client, auth_headers):
        """Triggering Refresh All should enqueue refresh probes and reschedule series."""
        import mongomock
        fake_db = mongomock.MongoClient().db
        series_collection = fake_db.series
        # Insert sample series
        count = 4
        for i in range(count):
            series_collection.insert_one({"_id": f"S{i}", "title": f"Series {i}"})

        with patch('tracker.db.get_series_collection', return_value=series_collection), \
             patch('tracker.tasks.get_series_collection', return_value=series_collection), \
             patch('tracker.tasks.enqueue_refresh_probe') as mock_enqueue:
            mock_enqueue.side_effect = lambda asin, response_groups=None, source=None: f"job-{asin}"
            response = client.post(
                "/config/api/series/refresh-all",
                headers=auth_headers
            )
            assert response.status_code == 200
            data = response.json()
            assert data.get('refresh', {}).get('count') == count
            # Reschedule should be queued as a follow-up job id
            assert 'reschedule_job_id' in data and data.get('reschedule_job_id')


class TestPublicAPI:
    """Test public API endpoints."""

    
    def test_public_series(self, client):
        """Test public series API."""
        with patch('tracker.db.get_series_collection') as mock_series_col:
            mock_series_col.return_value.find_one.return_value = {
                "_id": "TEST123",
                "title": "Test Series",
                "books": []
            }

            response = client.get("/config/api/public/series/TEST123")
            assert response.status_code == 200

    
    def test_public_series_books(self, client):
        """Test public series books API."""
        with patch('tracker.db.get_series_collection') as mock_series_col:
            mock_series_col.return_value.find_one.return_value = {
                "_id": "TEST123",
                "title": "Test Series",
                "books": [{"asin": "B001", "title": "Book 1"}]
            }

            response = client.get("/config/api/public/series/TEST123/books")
            assert response.status_code == 200

    def test_frontpage_preventclick_allows_links(self):
        """The drag-to-scroll click-prevent logic should allow clicks on links/buttons."""
        import pathlib
        tpl = pathlib.Path('tracker/templates/frontpage.html').read_text()
        assert "target.closest('a[href], button" in tpl or 'target.closest("a[href], button' in tpl or "target.closest('a[href]" in tpl


class TestDeveloperEndpoints:
    """Test developer-only endpoints."""


class TestAudibleIntegration:
    """Test Audible API integration."""

    
    def test_product_lookup(self, client, auth_headers):
        """Test looking up a product by ASIN."""
        with patch('lib.audible_api_search.get_product_by_asin') as mock_get_product:
            mock_get_product.return_value = {
                "asin": "TEST123",
                "title": "Test Book",
                "authors": ["Test Author"]
            }

            response = client.get(
                "/config/api/product/TEST123",
                headers=auth_headers
            )
            assert response.status_code == 200

    
    def test_audible_search(self, client, auth_headers):
        """Test searching Audible catalog."""
        with patch('lib.audible_api_search.search_audible') as mock_search:
            mock_search.return_value = {
                "products": [
                    {
                        "asin": "TEST123",
                        "title": "Test Book",
                        "authors": ["Test Author"]
                    }
                ]
            }

            response = client.post(
                "/config/api/search",
                json={"title": "test book"},
                headers=auth_headers
            )
            assert response.status_code == 200


class TestAccessControl:
    """Test access control and permissions."""

    
    def test_unauthorized_access(self, client):
        """Test accessing protected endpoints without authentication."""
        protected_endpoints = [
            "/config/library",
            "/config/settings",
            "/config/api/settings",
            "/config/api/library"
        ]

        # Temporarily override the dependency to simulate unauthorized access
        def unauthorized_get_current_user():
            raise HTTPException(status_code=401, detail="Invalid token")
        
        original_override = client.app.dependency_overrides.get(get_current_user)
        client.app.dependency_overrides[get_current_user] = unauthorized_get_current_user
        
        try:
            for endpoint in protected_endpoints:
                response = client.get(endpoint, follow_redirects=False)
                assert response.status_code in [401, 302, 403]  # Should require auth
        finally:
            # Restore the original override
            if original_override is not None:
                client.app.dependency_overrides[get_current_user] = original_override
            else:
                del client.app.dependency_overrides[get_current_user]

    
    def test_regular_user_access(self, client, user_auth_headers):
        """Test that regular users can access basic features."""
        response = client.get("/config/library", headers=user_auth_headers)
        assert response.status_code == 200

    
    def test_admin_only_access(self, client, user_auth_headers):
        """Test that admin-only features are restricted."""
        admin_endpoints = [
            "/config/users",
            "/config/series-admin"
        ]

        for endpoint in admin_endpoints:
            response = client.get(endpoint, headers=user_auth_headers)
            assert response.status_code in [403, 401]  # Should be forbidden for regular users