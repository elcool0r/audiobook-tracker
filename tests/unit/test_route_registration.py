"""FastAPI silently keeps both registrations of a duplicated method+path and only
ever matches the first, which is how POST /series/{asin}/refresh ended up with an
unreachable second handler that behaved differently from the one users hit."""
import collections

from tracker.api import api_router


def test_no_duplicate_method_and_path():
    seen = collections.Counter()
    for route in api_router.routes:
        for method in getattr(route, "methods", set()) - {"HEAD", "OPTIONS"}:
            seen[(method, route.path)] += 1
    duplicates = {key: count for key, count in seen.items() if count > 1}
    assert not duplicates, f"duplicate route registrations: {duplicates}"
