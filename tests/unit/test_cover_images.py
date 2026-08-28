"""Cover images must be stored as URLs, not embedded as base64 in the document.

A 60-book series carrying inline covers approached MongoDB's 16 MB document
limit, past which set_series_books raises DocumentTooLarge and the series
silently stops updating.
"""
import mongomock
import pytest
from unittest.mock import patch

from tracker.library import migrate_inline_cover_images


@pytest.fixture
def series():
    col = mongomock.MongoClient().db.series
    with patch("tracker.library.get_series_collection", return_value=col):
        yield col


TINY_PNG = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="


def test_migration_restores_urls_from_image_url(series):
    series.insert_one({
        "_id": "S1",
        "cover_image": TINY_PNG,
        "books": [
            {"asin": "B1", "image": TINY_PNG, "image_url": "https://img/1.jpg"},
            {"asin": "B2", "image": TINY_PNG, "image_url": "https://img/2.jpg"},
        ],
    })
    assert migrate_inline_cover_images() == 1

    doc = series.find_one({"_id": "S1"})
    assert [b["image"] for b in doc["books"]] == ["https://img/1.jpg", "https://img/2.jpg"]
    assert doc["cover_image"] == "https://img/1.jpg"


def test_migration_clears_covers_with_no_recoverable_url(series):
    series.insert_one({"_id": "S1", "cover_image": TINY_PNG,
                       "books": [{"asin": "B1", "image": TINY_PNG}]})
    migrate_inline_cover_images()

    doc = series.find_one({"_id": "S1"})
    assert doc["books"][0]["image"] is None
    assert doc["cover_image"] is None


def test_migration_leaves_url_covers_alone(series):
    series.insert_one({"_id": "S1", "cover_image": "https://img/1.jpg",
                       "books": [{"asin": "B1", "image": "https://img/1.jpg"}]})
    assert migrate_inline_cover_images() == 0
    assert series.find_one({"_id": "S1"})["books"][0]["image"] == "https://img/1.jpg"


def test_migration_skips_hidden_books_when_choosing_the_cover(series):
    series.insert_one({
        "_id": "S1",
        "cover_image": TINY_PNG,
        "books": [
            {"asin": "B1", "image": TINY_PNG, "image_url": "https://img/1.jpg", "hidden": True},
            {"asin": "B2", "image": TINY_PNG, "image_url": "https://img/2.jpg"},
        ],
    })
    migrate_inline_cover_images()
    assert series.find_one({"_id": "S1"})["cover_image"] == "https://img/2.jpg"


def test_migration_is_idempotent(series):
    series.insert_one({"_id": "S1", "cover_image": TINY_PNG,
                       "books": [{"asin": "B1", "image": TINY_PNG,
                                  "image_url": "https://img/1.jpg"}]})
    migrate_inline_cover_images()
    assert migrate_inline_cover_images() == 0


def test_fetch_path_does_not_embed_image_bytes():
    """Guards against reintroducing the inline-cover download."""
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
