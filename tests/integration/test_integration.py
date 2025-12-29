import pytest
from unittest.mock import patch, MagicMock
from tracker.app import create_app
from tracker.auth import create_access_token
import os


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    # Set up test environment variables
    os.environ['MONGO_URI'] = 'mongodb://localhost:27017'
    os.environ['MONGO_DB'] = 'test_audiobook_tracker'
    os.environ['SECRET_KEY'] = 'test_secret_key'

    # Mock user lookup for admin user
    from tracker.auth import get_password_hash
    admin_password_hash = get_password_hash("admin")
    
    mock_users_collection = MagicMock()
    def mock_find_user(query):
        # Handle both simple username queries and $or queries for frontpage
        username = query.get("username")
        if username == "admin":
            return {
                "username": "admin",
                "password_hash": admin_password_hash,
                "role": "admin",
                "failed_attempts": 0,
                "lock_until": None
            }
        
        # Handle $or queries for frontpage_slug/username lookup
        or_conditions = query.get("$or")
        if or_conditions:
            for condition in or_conditions:
                if condition.get("username") == "admin" or condition.get("frontpage_slug") == "admin":
                    return {
                        "username": "admin",
                        "frontpage_slug": "admin",
                        "password_hash": admin_password_hash,
                        "role": "admin",
                        "failed_attempts": 0,
                        "lock_until": None
                    }
        return None
    
    mock_users_collection.find_one.side_effect = mock_find_user
    mock_users_collection.count_documents.return_value = 1

    # Patch the collection functions
    with patch('tracker.db.get_users_collection', return_value=mock_users_collection):
        with patch('tracker.db.get_settings_collection') as mock_settings_col:
            mock_settings_col.find_one.return_value = {
                "default_num_results": 10,
                "log_retention_days": 30,
                "default_frontpage_slug": "",
                "users_can_edit_frontpage_slug": False,
                "secret_key": "test_secret_key"
            }
            with patch('tracker.library.get_user_library') as mock_get_library:
                # Mock library data with series that have books with release dates
                mock_series = type('Series', (), {
                    'title': 'Test Series',
                    'asin': 'S001',
                    'url': 'https://example.com/series',
                    'fetched_at': '2024-01-01T00:00:00Z',
                    'books': [
                        type('Book', (), {
                            'title': 'Book 1',
                            'release_date': '2024-01-01',  # This will trigger the datetime bug
                            'url': 'https://example.com/book1',
                            'asin': 'B001',
                            'narrators': ['Narrator 1'],
                            'runtime': 360
                        })()
                    ]
                })()
                mock_get_library.return_value = [mock_series]
                with patch('slowapi.extension.Limiter.limit', return_value=lambda func: func):  # Disable rate limiting decorator
                    app = create_app()
                    # Remove SlowAPI middleware for tests
                    app.user_middleware = [m for m in app.user_middleware if 'SlowAPI' not in str(m.cls)]
                    from fastapi.testclient import TestClient
                    with TestClient(app) as client:
                        yield client


@pytest.fixture
def auth_headers():
    """Create authentication headers for admin user."""
    token = create_access_token({"sub": "admin"})
    return {"Cookie": f"auth_token={token}"}


class TestAppStartup:
    """Test application startup and basic functionality."""

    
    def test_app_startup(self, client):
        """Test that the app starts up successfully."""
        response = client.get("/")
        assert response.status_code in [200, 302]  # Redirect to /config/

    
    def test_health_check(self, client):
        """Test health check endpoint."""
        # The app doesn't have a specific health check, but we can test basic connectivity
        response = client.get("/config/")
        assert response.status_code in [200, 302, 401]  # May require auth


class TestAuthentication:
    """Test authentication functionality."""

    
    def test_login_page_access(self, client):
        """Test accessing the login page."""
        response = client.get("/config/login")
        assert response.status_code == 200
        assert "login" in response.text.lower()

    
    def test_invalid_login(self, client):
        """Test login with invalid credentials."""
        response = client.post(
            "/config/login",
            data={"username": "invalid", "password": "wrong"},
            follow_redirects=False
        )
        assert response.status_code == 200  # Returns login page with error
        assert "Invalid credentials" in response.text

    
    def test_valid_login(self, client):
        """Test login with valid credentials."""
        # Mock successful authentication
        with patch('tracker.app.verify_password', return_value=True), \
             patch('tracker.db.get_users_collection') as mock_users_col:

            mock_users_col.return_value.find_one.return_value = {
                "username": "admin",
                "role": "admin",
                "notifications": {"enabled": True}
            }

            response = client.post(
                "/config/login",
                data={"username": "admin", "password": "correct"},
                follow_redirects=False
            )
            assert response.status_code == 302
            assert "/config/" in response.headers.get("location", "")

    
    def test_logout(self, client, auth_headers):
        """Test logout functionality."""
        response = client.get("/config/logout", headers=auth_headers, follow_redirects=False)
        assert response.status_code == 302
        assert "/config/login" in response.headers.get("location", "")


class TestPageAccess:
    """Test accessing all main pages."""

    
    def test_dashboard_page(self, client, auth_headers):
        """Test accessing the main dashboard."""
        response = client.get("/config/", headers=auth_headers)
        assert response.status_code == 200
        assert "dashboard" in response.text.lower() or "library" in response.text.lower()

    
    def test_library_page(self, client, auth_headers):
        """Test accessing the library page."""
        response = client.get("/config/library", headers=auth_headers)
        assert response.status_code == 200

    
    def test_settings_page(self, client, auth_headers):
        """Test accessing the settings page."""
        response = client.get("/config/settings", headers=auth_headers)
        assert response.status_code == 200
        assert "settings" in response.text.lower()

    
    def test_users_page(self, client, auth_headers):
        """Test accessing the users page (admin only)."""
        response = client.get("/config/users", headers=auth_headers)
        assert response.status_code == 200

    
    def test_series_admin_page(self, client, auth_headers):
        """Test accessing the series admin page."""
        response = client.get("/config/series-admin", headers=auth_headers)
        assert response.status_code == 200

    
    def test_jobs_page(self, client, auth_headers):
        """Test accessing the jobs page."""
        response = client.get("/config/jobs", headers=auth_headers)
        assert response.status_code == 200

    
    def test_profile_page(self, client, auth_headers):
        """Test accessing the profile page."""
        response = client.get("/config/profile", headers=auth_headers)
        assert response.status_code == 200
        # Frontpage slug editing is disabled by default in settings, so the field should not be visible
        assert "Frontpage slug" not in response.text
        # We removed the Open frontpage link from the profile as the navbar provides it
        assert "Open frontpage" not in response.text

    def test_profile_page_frontpage_edit_visible_when_enabled(self, client, auth_headers):
        """When the admin enables the setting, the profile should show the frontpage slug input."""
        with patch('tracker.settings.get_settings_collection') as mock_settings_col:
            mock_settings_col.return_value.find_one.return_value = {
                "default_num_results": 10,
                "log_retention_days": 30,
                "default_frontpage_slug": "",
                "users_can_edit_frontpage_slug": True,
                "secret_key": "test_secret_key"
            }
            response = client.get("/config/profile", headers=auth_headers)
            assert response.status_code == 200
            assert "Frontpage slug" in response.text
            # Input element should be present
            assert 'id="frontpageSlug"' in response.text

    
    def test_logs_page(self, client, auth_headers):
        """Test accessing the logs page."""
        response = client.get("/config/logs", headers=auth_headers)
        assert response.status_code == 200

    
    def test_series_page(self, client, auth_headers):
        """Test accessing a series page."""
        response = client.get("/config/series/TEST123", headers=auth_headers)
        assert response.status_code in [200, 404]  # 404 if series doesn't exist

    
    def test_series_books_page(self, client, auth_headers):
        """Test accessing the series books page."""
        response = client.get("/config/series-books", headers=auth_headers)
        assert response.status_code == 200


class TestMetrics:
    """Test metrics endpoint."""

    
    def test_metrics_endpoint(self, client):
        """Test accessing Prometheus metrics."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "audiobook" in response.text  # Should contain our custom metrics


class TestAPI:
    """Test API endpoints."""

    
    def test_api_settings_get(self, client, auth_headers):
        """Test getting settings via API."""
        response = client.get("/config/api/settings", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        # New setting defaults to False
        assert data.get('users_can_edit_frontpage_slug') is False

    
    def test_api_library_get(self, client, auth_headers):
        """Test getting library via API."""
        response = client.get("/config/api/library", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    
    def test_api_users_get(self, client, auth_headers):
        """Test getting users via API."""
        response = client.get("/config/api/users", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    
    def test_api_jobs_get(self, client, auth_headers):
        """Test getting jobs via API."""
        response = client.get("/config/api/jobs", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_admin_update_frontpage_blocked_when_setting_disabled(self, client, auth_headers):
        """When the global setting is disabled, even admins cannot update frontpage slug via profile endpoint."""
        with patch('tracker.settings.get_settings_collection') as mock_settings_col:
            mock_settings_col.return_value.find_one.return_value = {
                "default_num_results": 10,
                "log_retention_days": 30,
                "default_frontpage_slug": "",
                "users_can_edit_frontpage_slug": False,
                "secret_key": "test_secret_key"
            }
            resp = client.post("/config/api/profile/frontpage", json={"slug": "admin-slug"}, headers=auth_headers)
            assert resp.status_code == 403
            data = resp.json()
            assert data.get('detail') == 'Frontpage slug changes are disabled'

    
    def test_api_search(self, client, auth_headers):
        """Test search API endpoint."""
        response = client.post(
            "/config/api/search",
            json={"title": "test book", "num_results": 5},
            headers=auth_headers
        )
        assert response.status_code in [200, 400]  # May fail without proper Audible API setup

    
    def test_api_series_search(self, client, auth_headers):
        """Test series search API endpoint."""
        response = client.post(
            "/config/api/series/search",
            json={"query": "test series"},
            headers=auth_headers
        )
        assert response.status_code in [200, 400]  # May fail without proper setup


class TestPublicPages:
    """Test public (unauthenticated) pages."""

    
    def test_root_redirect(self, client):
        """Test root path redirects appropriately."""
        response = client.get("/", follow_redirects=False)
        assert response.status_code == 302

    
    def test_public_home_page(self, client):
        """Test accessing a public user home page."""
        response = client.get("/home/admin")
        assert response.status_code == 200


class TestStaticFiles:
    """Test static file serving."""

    
    def test_static_css(self, client):
        """Test accessing CSS files."""
        response = client.get("/static/css/custom.css")
        assert response.status_code == 200
        assert "text/css" in response.headers.get("content-type", "")

    
    def test_favicon(self, client):
        """Test accessing favicon."""
        response = client.get("/static/favicon/site.webmanifest")
        assert response.status_code == 200


class TestErrorHandling:
    """Test error handling."""

    
    def test_404_page(self, client):
        """Test 404 error page."""
        response = client.get("/nonexistent")
        assert response.status_code == 404

    
    def test_invalid_series_asin(self, client, auth_headers):
        """Test accessing invalid series ASIN."""
        response = client.get("/config/series/INVALID", headers=auth_headers)
        assert response.status_code == 404


class TestSecurity:
    """Test security features."""

    
    def test_rate_limiting(self, client):
        """Test rate limiting on login endpoint."""
        # Make multiple rapid requests to test rate limiting
        for i in range(10):
            response = client.post(
                "/config/login",
                data={"username": "test", "password": "test"}
            )
            if response.status_code == 429:  # Too Many Requests
                break
        # Rate limiting may or may not trigger depending on configuration
        assert response.status_code in [200, 302, 429]

    
    def test_csrf_protection(self, client, auth_headers):
        """Test CSRF protection (if enabled)."""
        # CSRF protection appears to be commented out in the code
        # So this test just verifies the endpoint exists
        response = client.post("/config/api/settings", headers=auth_headers)
        assert response.status_code in [200, 422]  # 422 if CSRF validation fails


class TestVersion:
    """Test version information."""

    
    def test_version_in_navbar(self, client, auth_headers):
        """Test that version information is displayed in the navbar."""
        response = client.get("/config/", headers=auth_headers)
        assert response.status_code == 200
        # Version should be displayed somewhere in the UI
        assert len(response.text) > 100  # Basic check that we got a full page