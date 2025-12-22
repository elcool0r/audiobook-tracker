from fastapi.testclient import TestClient
import sys
from pathlib import Path

# Make imports work when running this file directly from repo root
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    # Prefer package import when run as module
    from tracker.app import app
except Exception:
    # Fallback to local import when running this file directly
    from app import app

client = TestClient(app)

r = client.get('/login')
print('GET /login', r.status_code)

# try login with default admin/admin
r = client.post('/login', data={'username':'admin', 'password':'admin'})
print('POST /login', r.status_code, r.headers.get('location'))

# try to access dashboard (should redirect or require cookie)
r = client.get('/dashboard')
print('GET /dashboard', r.status_code)

# library endpoints
r = client.get('/api/library')
print('GET /api/library', r.status_code)
