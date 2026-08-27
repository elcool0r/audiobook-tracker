"""Session-wide pytest configuration.

`tracker.app` builds the application at import time, which opens a database
connection, so the in-memory backend has to be enabled before any test module is
imported. Production refuses to start without a reachable MONGO_URI (see
tracker/db.py); tests opt in explicitly here.
"""
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault("ALLOW_IN_MEMORY_DB", "1")
os.environ.setdefault("SECRET_KEY", "test_secret_key")
