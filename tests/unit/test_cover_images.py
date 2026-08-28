"""Cover images are cached to local disk, never embedded in the document and
never left pointing at a bare remote URL for the browser to fetch directly.

Covers change out from under a fixed Audible URL from time to time, so caching
must revalidate on every full refresh, not just cache-once.
"""
import mongomock
import pytest
from unittest.mock import patch, MagicMock

from tracker import covers
from tracker.library import migrate_cover_images_to_local_cache, set_series_books


TINY_PNG_DATA_URI = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="


def _response(status=200, content=b"bytes", content_type="image/jpeg", etag=None):
    resp = MagicMock()
    resp.status_code = status
    resp.content = content
    headers = {"Content-Type": content_type}
    if etag:
        headers["ETag"] = etag
    resp.headers = headers
    return resp


@pytest.fixture
def covers_tmp_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("COVERS_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def series(covers_tmp_dir):
    col = mongomock.MongoClient().db.series
    with patch("tracker.library.get_series_collection", return_value=col):
        yield col


class TestCacheCover:
    def test_downloads_and_writes_a_local_file(self, covers_tmp_dir):
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')) as get:
            local_path, key, etag = covers.cache_cover("https://img.example/cover.jpg")
        assert local_path.startswith("/covers/")
        assert etag == '"v1"'
        assert (covers_tmp_dir / f"{key}.jpg").read_bytes() == b"bytes"
        get.assert_called_once()

    def test_revalidation_sends_the_prior_etag(self, covers_tmp_dir):
        url = "https://img.example/cover.jpg"
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')):
            _, key, etag = covers.cache_cover(url)

        with patch.object(covers._SESSION, "get") as get:
            get.return_value = _response(status=304)
            local_path, new_key, new_etag = covers.cache_cover(
                url, previous_key=key, previous_etag=etag
            )
        assert get.call_args.kwargs["headers"] == {"If-None-Match": '"v1"'}
        assert new_key == key
        assert new_etag == etag
        assert local_path == f"/covers/{key}.jpg"

    def test_a_changed_cover_replaces_the_file_and_etag(self, covers_tmp_dir):
        url = "https://img.example/cover.jpg"
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')):
            _, key, _ = covers.cache_cover(url)

        with patch.object(covers._SESSION, "get", return_value=_response(content=b"new bytes", etag='"v2"')):
            local_path, new_key, new_etag = covers.cache_cover(
                url, previous_key=key, previous_etag='"v1"'
            )
        assert new_key == key  # same URL -> same cache key
        assert new_etag == '"v2"'
        assert (covers_tmp_dir / f"{key}.jpg").read_bytes() == b"new bytes"

    def test_a_changed_url_downloads_fresh_and_removes_the_old_file(self, covers_tmp_dir):
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')):
            _, old_key, old_etag = covers.cache_cover("https://img.example/old.jpg")
        assert (covers_tmp_dir / f"{old_key}.jpg").exists()

        with patch.object(covers._SESSION, "get", return_value=_response(content=b"different")):
            local_path, new_key, _ = covers.cache_cover(
                "https://img.example/new.jpg", previous_key=old_key, previous_etag=old_etag
            )
        assert new_key != old_key
        assert not (covers_tmp_dir / f"{old_key}.jpg").exists()
        assert (covers_tmp_dir / f"{new_key}.jpg").read_bytes() == b"different"

    def test_fetch_failure_falls_back_to_the_previous_state(self, covers_tmp_dir):
        with patch.object(covers._SESSION, "get", side_effect=Exception("network down")):
            local_path, key, etag = covers.cache_cover(
                "https://img.example/cover.jpg", previous_key="oldkey", previous_etag='"v1"'
            )
        assert local_path is None
        assert key == "oldkey"
        assert etag == '"v1"'

    def test_no_url_returns_nothing(self):
        assert covers.cache_cover(None) == (None, None, None)
        assert covers.cache_cover("") == (None, None, None)


class TestSetSeriesBooksCaching:
    def test_new_book_gets_its_cover_cached_locally(self, series):
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')):
            processed = set_series_books("S1", [
                {"asin": "B1", "title": "Book One", "image": "https://img.example/b1.jpg"},
            ])
        assert processed[0]["image"].startswith("/covers/")
        assert processed[0]["image_url"] == "https://img.example/b1.jpg"
        assert processed[0]["image_etag"] == '"v1"'

    def test_a_full_refetch_revalidates_rather_than_redownloading(self, series):
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')) as get:
            set_series_books("S1", [
                {"asin": "B1", "title": "Book One", "image": "https://img.example/b1.jpg"},
            ])
        assert get.call_count == 1

        with patch.object(covers._SESSION, "get", return_value=_response(status=304)) as get2:
            set_series_books("S1", [
                {"asin": "B1", "title": "Book One", "image": "https://img.example/b1.jpg"},
            ])
        # Revalidated (one conditional request), not re-downloaded from scratch.
        assert get2.call_count == 1
        assert get2.call_args.kwargs["headers"].get("If-None-Match") == '"v1"'

    def test_a_book_dropped_from_the_series_has_its_cover_file_removed(self, series, covers_tmp_dir):
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')):
            processed = set_series_books("S1", [
                {"asin": "B1", "title": "Book One", "image": "https://img.example/b1.jpg"},
            ])
        key = processed[0]["image_cache_key"]
        assert (covers_tmp_dir / f"{key}.jpg").exists()

        with patch.object(covers._SESSION, "get", return_value=_response()):
            set_series_books("S1", [
                {"asin": "B2", "title": "Book Two", "image": "https://img.example/b2.jpg"},
            ])
        assert not (covers_tmp_dir / f"{key}.jpg").exists()

    def test_cache_failure_keeps_the_existing_local_cover(self, series, covers_tmp_dir):
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')):
            processed = set_series_books("S1", [
                {"asin": "B1", "title": "Book One", "image": "https://img.example/b1.jpg"},
            ])
        cached_path = processed[0]["image"]
        assert cached_path.startswith("/covers/")

        with patch.object(covers._SESSION, "get", side_effect=Exception("network down")):
            processed2 = set_series_books("S1", [
                {"asin": "B1", "title": "Book One", "image": "https://img.example/b1.jpg"},
            ])
        assert processed2[0]["image"] == cached_path


class TestMigration:
    def test_recovers_from_data_uri_then_caches_locally(self, series):
        series.insert_one({
            "_id": "S1",
            "cover_image": TINY_PNG_DATA_URI,
            "books": [
                {"asin": "B1", "image": TINY_PNG_DATA_URI, "image_url": "https://img.example/b1.jpg"},
            ],
        })
        with patch.object(covers._SESSION, "get", return_value=_response(etag='"v1"')):
            updated = migrate_cover_images_to_local_cache()
        assert updated == 1

        doc = series.find_one({"_id": "S1"})
        assert doc["books"][0]["image"].startswith("/covers/")
        assert doc["cover_image"].startswith("/covers/")

    def test_caches_a_bare_remote_url(self, series):
        series.insert_one({
            "_id": "S1", "cover_image": "https://img.example/b1.jpg",
            "books": [{"asin": "B1", "image": "https://img.example/b1.jpg"}],
        })
        with patch.object(covers._SESSION, "get", return_value=_response()):
            assert migrate_cover_images_to_local_cache() == 1
        assert series.find_one({"_id": "S1"})["books"][0]["image"].startswith("/covers/")

    def test_already_migrated_series_are_left_alone(self, series):
        series.insert_one({
            "_id": "S1", "cover_image": "/covers/abc.jpg",
            "books": [{"asin": "B1", "image": "/covers/abc.jpg"}],
        })
        with patch.object(covers._SESSION, "get") as get:
            assert migrate_cover_images_to_local_cache() == 0
        get.assert_not_called()

    def test_data_uri_with_no_recoverable_url_is_cleared_not_re_downloaded(self, series):
        series.insert_one({"_id": "S1", "cover_image": TINY_PNG_DATA_URI,
                           "books": [{"asin": "B1", "image": TINY_PNG_DATA_URI}]})
        with patch.object(covers._SESSION, "get") as get:
            migrate_cover_images_to_local_cache()
        assert series.find_one({"_id": "S1"})["books"][0]["image"] is None
        get.assert_not_called()


def test_fetch_path_does_not_embed_image_bytes():
    """Guards against reintroducing the inline-cover download in library.py."""
    import inspect
    from tracker import library

    source = inspect.getsource(library._fetch_series_books_internal)
    assert "b64encode" not in source, "cover bytes are being embedded again"


def test_book_summary_keeps_the_cover_as_a_url():
    from tracker.library import _book_summary

    book = _book_summary({
        "asin": "B1",
        "title": "Book One",
        "product_images": {"500": "https://m.media-amazon.com/images/I/abc._SL500_.jpg"},
    })
    assert book["image"] == "https://m.media-amazon.com/images/I/abc._SL500_.jpg"
    assert not str(book["image"]).startswith("data:")
