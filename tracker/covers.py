"""Local disk cache for book cover images.

Covers are downloaded once and served from local disk under /covers, instead of
either embedding the bytes in MongoDB (see the base64 removal in library.py) or
having every viewer's browser fetch them directly from Audible on each page
load. Every time a series gets a full book refetch, cache_cover() is called
again with the previous cache key/ETag; a conditional GET means an unchanged
cover costs a 304 and no bandwidth, while a changed URL or changed ETag pulls
fresh bytes and drops the stale file.
"""
from __future__ import annotations

import hashlib
import logging
import mimetypes
import os
from pathlib import Path
from typing import Optional, Tuple

from lib.audible_api_search import _SESSION

logger = logging.getLogger(__name__)

DEFAULT_COVERS_DIR = Path(__file__).resolve().parent.parent / "data" / "covers"
_DEFAULT_EXT = ".jpg"
REQUEST_TIMEOUT_SECONDS = 15
# Served path prefix; must match the StaticFiles mount in tracker.app.
URL_PREFIX = "/covers"


def covers_dir() -> Path:
    path = Path(os.getenv("COVERS_DIR") or DEFAULT_COVERS_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path


def cache_key_for_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]


def _extension_for(content_type: str | None, url: str) -> str:
    if content_type:
        ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext:
            # .jpe is a legitimate but unusual guess for image/jpeg on some platforms.
            return ".jpg" if ext == ".jpe" else ext
    guessed_type, _ = mimetypes.guess_type(url)
    if guessed_type:
        ext = mimetypes.guess_extension(guessed_type)
        if ext:
            return ext
    return _DEFAULT_EXT


def _find_existing(key: str) -> Optional[Path]:
    for candidate in covers_dir().glob(f"{key}.*"):
        return candidate
    return None


def local_path_for_key(key: str) -> Optional[str]:
    existing = _find_existing(key)
    return f"{URL_PREFIX}/{existing.name}" if existing else None


def delete_cached_cover(key: str | None) -> None:
    if not key:
        return
    existing = _find_existing(key)
    if existing is not None:
        try:
            existing.unlink()
        except OSError:
            logger.warning("Failed to remove cached cover %s", existing)


def cache_cover(
    url: str | None,
    *,
    previous_key: str | None = None,
    previous_etag: str | None = None,
    proxies: dict | None = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Ensure `url` is cached locally and return (local_url_path, cache_key, etag).

    On any failure, returns (None, previous_key, previous_etag) so the caller can
    fall back to an already-cached copy, or to the remote URL, without losing
    track of what was cached before.
    """
    if not url:
        return None, None, None

    key = cache_key_for_url(url)
    headers: dict[str, str] = {}
    # Only send a conditional header when the URL matches what produced that
    # ETag; a URL change always needs a fresh, unconditional fetch.
    reusable_etag = previous_etag if previous_key == key else None
    if reusable_etag:
        headers["If-None-Match"] = reusable_etag

    try:
        resp = _SESSION.get(url, timeout=REQUEST_TIMEOUT_SECONDS, proxies=proxies, headers=headers)
    except Exception:
        logger.warning("Failed to fetch cover %s", url, exc_info=True)
        return None, previous_key, previous_etag

    if resp.status_code == 304 and reusable_etag:
        existing = _find_existing(key)
        if existing is not None:
            return f"{URL_PREFIX}/{existing.name}", key, reusable_etag
        # We believed we had a cached file but it is gone; fall through and
        # re-fetch unconditionally below by treating this as a cache miss.
        try:
            resp = _SESSION.get(url, timeout=REQUEST_TIMEOUT_SECONDS, proxies=proxies)
        except Exception:
            logger.warning("Failed to re-fetch cover %s after cache miss", url, exc_info=True)
            return None, previous_key, previous_etag

    if resp.status_code != 200:
        logger.warning("Cover fetch for %s returned HTTP %s", url, resp.status_code)
        return None, previous_key, previous_etag

    ext = _extension_for(resp.headers.get("Content-Type"), url)
    target = covers_dir() / f"{key}{ext}"
    try:
        target.write_bytes(resp.content)
    except OSError:
        logger.warning("Failed to write cover cache file %s", target, exc_info=True)
        return None, previous_key, previous_etag

    # A different extension guess than a prior run would leave an orphaned file
    # under the same key; only one file per key should ever exist.
    for stale in covers_dir().glob(f"{key}.*"):
        if stale != target:
            try:
                stale.unlink()
            except OSError:
                pass

    if previous_key and previous_key != key:
        delete_cached_cover(previous_key)

    return f"{URL_PREFIX}/{target.name}", key, resp.headers.get("ETag")
