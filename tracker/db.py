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


@lru_cache()
def get_db():
    uri = os.getenv("MONGO_URI")
    if uri:
        # Try connecting with a short timeout; if unavailable, fall back to mongomock for tests
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            # Quick ping to detect unreachable servers
            client.admin.command('ping')
        except Exception:
            if mongomock is not None:
                client = mongomock.MongoClient()
            else:
                # Re-raise original exception to make failure explicit when mongomock isn't available
                raise
    else:
        if mongomock is None:
            raise RuntimeError("MONGO_URI not set and mongomock is not installed; please install mongomock or set MONGO_URI")
        client = mongomock.MongoClient()
    db_name = os.getenv("MONGO_DB", "audiobook_tracker")
    return client[db_name]


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
