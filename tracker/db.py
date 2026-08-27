import logging
import warnings
from functools import lru_cache
import os

from pymongo import MongoClient

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r".*pkg_resources is deprecated.*",
    module="mongomock.__version__",
)

try:
    import mongomock  # type: ignore
except ImportError:  # fallback if mongomock is not installed
    mongomock = None


def _in_memory_db_allowed() -> bool:
    return os.getenv("ALLOW_IN_MEMORY_DB", "").strip().lower() in {"1", "true", "yes"}


@lru_cache()
def get_db():
    """Return the application database.

    A real MONGO_URI must reach a real server. Falling back to an in-memory
    database on a connection error would let the app start and accept writes that
    are silently discarded on restart, so connection failures are raised instead.
    The in-memory backend is opt-in via ALLOW_IN_MEMORY_DB and is meant for tests.
    """
    db_name = os.getenv("MONGO_DB", "audiobook_tracker")
    uri = os.getenv("MONGO_URI")
    if uri:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        # Fail fast and loudly rather than degrading to a throwaway database.
        client.admin.command("ping")
        return client[db_name]

    if not _in_memory_db_allowed():
        raise RuntimeError(
            "MONGO_URI is not set. Set MONGO_URI to a MongoDB connection string, or set "
            "ALLOW_IN_MEMORY_DB=1 to use a non-persistent in-memory database (tests only)."
        )
    if mongomock is None:
        raise RuntimeError("ALLOW_IN_MEMORY_DB is set but mongomock is not installed.")
    logging.warning(
        "Using an in-memory database (ALLOW_IN_MEMORY_DB): data will NOT persist across restarts."
    )
    return mongomock.MongoClient()[db_name]


def get_series_collection():
    return get_db()["series"]


def get_user_library_collection():
    return get_db()["user_library"]


def get_users_collection():
    return get_db()["users"]


def get_settings_collection():
    return get_db()["settings"]


def get_jobs_collection():
    return get_db()["jobs"]


def get_api_keys_collection():
    return get_db()["api_keys"]


def get_logs_collection():
    return get_db()["logs"]
