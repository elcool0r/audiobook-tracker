from __future__ import annotations

import logging
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import base64
import re
import requests

from pydantic import BaseModel, ConfigDict, Field

from lib.audible_api_search import DEFAULT_RESPONSE_GROUPS, get_product_by_asin

from pymongo import ASCENDING, UpdateOne

from .db import get_series_collection, get_user_library_collection, get_users_collection, get_jobs_collection


class LibraryBook(BaseModel):
    title: Optional[str] = None
    asin: Optional[str] = None
    url: Optional[str] = None
    release_date: Optional[str] = None
    runtime: Optional[str | int] = None
    narrators: Optional[str] = None
    image: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None
    hidden: bool = False

    model_config = ConfigDict(extra="ignore")


class LibraryItem(BaseModel):
    title: str
    asin: Optional[str] = None
    url: Optional[str] = None
    books: List[LibraryBook] = Field(default_factory=list)
    added_at: Optional[str] = None
    fetched_at: Optional[str] = None
    username: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


def _now_iso() -> str:
    iso = datetime.now(timezone.utc).isoformat()
    return iso.replace("+00:00", "Z")


def _series_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "asin": doc.get("_id"),
        "title": doc.get("title"),
        "url": doc.get("url"),
        "books": doc.get("books", []),
        "cover_image": doc.get("cover_image"),
        "fetched_at": doc.get("fetched_at"),
        "raw": doc.get("raw"),
        "next_refresh_at": doc.get("next_refresh_at"),
        "user_count": doc.get("user_count", 0),
        "original_title": doc.get("original_title"),
        "narrator_warnings": doc.get("narrator_warnings", []),
        "ignore_narrator_warnings": bool(doc.get("ignore_narrator_warnings", False)),
    }


def _library_item_from_series_payload(payload: Dict[str, Any], entry: Dict[str, Any], username: str) -> LibraryItem:
    merged = payload.copy()
    merged["added_at"] = entry.get("added_at")
    merged["username"] = username
    return LibraryItem(**merged)


def _manual_library_item(entry: Dict[str, Any], username: str) -> LibraryItem:
    return LibraryItem(
        title=entry.get("title") or "Untitled",
        asin=entry.get("series_asin"),
        url=entry.get("url"),
        books=entry.get("books") or [],
        added_at=entry.get("added_at"),
        fetched_at=entry.get("fetched_at"),
        username=username,
    )


def _book_identity(book: Dict[str, Any] | None) -> str | None:
    if not isinstance(book, dict):
        return None
    asin = book.get("asin")
    if asin:
        return f"asin:{asin}"
    title = book.get("title")
    if isinstance(title, str) and title.strip():
        return f"title:{title.strip().lower()}"
    return None


def is_book_hidden(book: Dict[str, Any] | BaseModel | None) -> bool:
    if isinstance(book, BaseModel):
        return bool(getattr(book, "hidden", False))
    if isinstance(book, dict):
        return bool(book.get("hidden"))
    return False


def visible_books(books: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    if not isinstance(books, list):
        return []
    return [b for b in books if not is_book_hidden(b)]


def visible_book_count(books: List[Dict[str, Any]] | None) -> int:
    return len(visible_books(books))


def _get_primary_narrator(narrators: Any) -> str | None:
    if isinstance(narrators, list):
        if not narrators:
            return None
        first = narrators[0]
        if isinstance(first, dict):
            name = first.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
            return None
        if isinstance(first, str):
            candidate = first.strip()
            return candidate if candidate else None
        return None
    if isinstance(narrators, str):
        parts = [part.strip() for part in narrators.split(",") if part.strip()]
        return parts[0] if parts else None
    return None


def _book_sequence(book: Dict[str, Any], series_asin: str | None) -> int:
    if not isinstance(book, dict):
        return 999
    for entry in book.get("series", []) or []:
        if not isinstance(entry, dict):
            continue
        if entry.get("asin") == series_asin:
            seq = entry.get("sequence")
            try:
                return int(seq)
            except Exception:
                return 999
    return 999


def compute_narrator_warnings(books: List[Dict[str, Any]] | None, series_asin: str | None) -> List[str]:
    if not isinstance(books, list) or not books:
        return []
    valid_books = [book for book in books if isinstance(book, dict)]
    if not valid_books:
        return []

    def _release_dt(book):
        rd = book.get("release_date")
        if isinstance(rd, str) and rd.strip():
            try:
                return datetime.fromisoformat(rd.split("T")[0])
            except Exception:
                return None
        return None

    # Sort primarily by declared sequence (if present), falling back to release date so the first book is the earliest by sequence/date
    sorted_books = sorted(valid_books, key=lambda book: (_book_sequence(book, series_asin), _release_dt(book) or datetime.max))
    first_book = sorted_books[0]
    primary_narrator = _get_primary_narrator(first_book.get("narrators"))
    if not primary_narrator:
        return []
    warnings: List[str] = []
    for book in sorted_books[1:]:
        if book.get("ignore_narrator_warning"):
            continue
        book_narrator = _get_primary_narrator(book.get("narrators"))
        if book_narrator != primary_narrator:
            warnings.append(book.get("title", "Unknown"))
    return warnings


def ensure_series_document(asin: str, title: Optional[str], url: Optional[str]) -> tuple[Dict[str, Any], bool]:
    series_col = get_series_collection()
    existing = series_col.find_one({"_id": asin})
    now = _now_iso()
    if existing:
        update: Dict[str, Any] = {}
        if title and existing.get("title") != title:
            update["title"] = title
        if url and existing.get("url") != url:
            update["url"] = url
        if update:
            update["updated_at"] = now
            series_col.update_one({"_id": asin}, {"$set": update})
        existing = series_col.find_one({"_id": asin})
        return _series_payload(existing), False
    doc = {
        "_id": asin,
        "title": title,
        "url": url,
        "books": [],
        "created_at": now,
        "fetched_at": None,
        "raw": None,
        "next_refresh_at": None,
        "user_count": 0,
    }
    series_col.insert_one(doc)
    return _series_payload(doc), True


def get_series_document(asin: str) -> Optional[Dict[str, Any]]:
    doc = get_series_collection().find_one({"_id": asin})
    if not doc:
        return None
    return _series_payload(doc)


def _deduplicate_books_by_title(books: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate books by title, keeping only the newer one based on release_date."""
    if not books:
        return []
    
    from datetime import datetime
    
    def _parse_date(date_str: str | None) -> datetime | None:
        """Parse date string (YYYY-MM-DD format) to datetime."""
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            return datetime.fromisoformat(date_str.split("T")[0])
        except Exception:
            return None
    
    # Group books by title (case-insensitive)
    books_by_title: Dict[str, List[Dict[str, Any]]] = {}
    for book in books:
        title = book.get("title", "").lower() if book.get("title") else ""
        if title:
            if title not in books_by_title:
                books_by_title[title] = []
            books_by_title[title].append(book)
    
    # For each title group, keep only the one with the latest release_date
    deduped = []
    for title, group in books_by_title.items():
        if len(group) == 1:
            deduped.append(group[0])
        else:
            # Sort by release_date descending (newest first), keeping originals for None dates
            def _sort_key(book):
                rd = _parse_date(book.get("release_date"))
                return (rd is not None, rd) if rd else (False, datetime.min)
            
            group.sort(key=_sort_key, reverse=True)
            deduped.append(group[0])  # Keep the newest
    
    return deduped


def set_series_books(asin: str, books: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if books is None:
        books = []
    # Deduplicate books by title, keeping only the newer one based on release_date
    books = _deduplicate_books_by_title(books)
    series_col = get_series_collection()
    existing_doc = series_col.find_one({"_id": asin}, {"books": 1}) or {}
    existing_books = existing_doc.get("books", []) if isinstance(existing_doc.get("books"), list) else []
    existing_map: Dict[str, Dict[str, Any]] = {}
    for book in existing_books:
        key = _book_identity(book)
        if key and key not in existing_map:
            existing_map[key] = book

    # Detect a series-level ignore flag so new books inherit ignore behavior
    series_col = get_series_collection()
    series_doc = series_col.find_one({"_id": asin}, {"ignore_narrator_warnings": 1}) or {}
    series_ignore = bool(series_doc.get("ignore_narrator_warnings", False))

    processed_books: List[Dict[str, Any]] = []
    for book in books:
        if not isinstance(book, dict):
            continue
        key = _book_identity(book)
        existing_hidden = existing_map.get(key, {}).get("hidden") if key else None
        incoming_hidden = book.get("hidden")
        if isinstance(incoming_hidden, bool):
            book_hidden = incoming_hidden
        else:
            book_hidden = bool(existing_hidden)
        book["hidden"] = book_hidden
        # Preserve any existing ignore_narrator_warning flag unless explicitly provided
        existing_ignore = existing_map.get(key, {}).get("ignore_narrator_warning") if key else None
        incoming_ignore = book.get("ignore_narrator_warning")
        if isinstance(incoming_ignore, bool):
            book_ignore = incoming_ignore
        else:
            book_ignore = bool(existing_ignore)
        # If series-level ignore is enabled, ensure the book is ignored and mark why
        if series_ignore:
            book_ignore = True
            # mark that this ignore was set due to series-level toggle so it can be reverted later
            book["ignore_narrator_warning_set_by_series"] = True
        book["ignore_narrator_warning"] = book_ignore
        processed_books.append(book)

    cover_image = None
    for book in processed_books:
        if book.get("hidden"):
            continue
        img = book.get("image")
        if img:
            cover_image = img
            break

    # Sort processed books so UI and computations have deterministic ordering: by sequence then by release date (oldest first)
    def _parse_date_str(ds):
        if isinstance(ds, str) and ds.strip():
            try:
                return datetime.fromisoformat(ds.split("T")[0])
            except Exception:
                return None
        return None

    processed_books.sort(key=lambda b: (_book_sequence(b, asin), _parse_date_str(b.get("release_date")) or datetime.max))

    # Only update books/fetched_at; don't create new docs (series doc should exist from ensure_series_document)
    result = series_col.update_one(
        {"_id": asin},
        {"$set": {"books": processed_books, "fetched_at": _now_iso(), "cover_image": cover_image}},
    )
    # If series doesn't exist, this is an error condition - log or handle
    if result.matched_count == 0:
        # Fallback: create minimal doc, but this shouldn't happen in normal flow
        series_col.update_one(
            {"_id": asin},
            {
                "$set": {"books": processed_books, "fetched_at": _now_iso()},
                "$setOnInsert": {"title": f"Series {asin}", "url": None, "created_at": _now_iso()}
            },
            upsert=True
        )
    return processed_books


def set_series_raw(asin: str, raw: Dict[str, Any] | None) -> None:
    # Avoid creating/upserting placeholder series entries
    if isinstance(raw, dict) and raw.get("issue_date") == "2200-01-01":
        return
    series_col = get_series_collection()
    series_col.update_one({"_id": asin}, {"$set": {"raw": raw}}, upsert=True)


def touch_series_fetched(asin: str) -> None:
    series_col = get_series_collection()
    series_col.update_one({"_id": asin}, {"$set": {"fetched_at": _now_iso()}}, upsert=True)


def set_series_next_refresh(asin: str, when_iso: str | None) -> None:
    series_col = get_series_collection()
    series_col.update_one({"_id": asin}, {"$set": {"next_refresh_at": when_iso}}, upsert=True)


def get_user_library(username: str) -> List[LibraryItem]:
    user_col = get_user_library_collection()
    series_col = get_series_collection()
    entries = list(user_col.find({"username": username}))
    if not entries:
        return []
    series_asins = [entry.get("series_asin") for entry in entries if entry.get("series_asin")]
    docs = list(series_col.find({"_id": {"$in": series_asins}})) if series_asins else []
    series_map = {doc["_id"]: _series_payload(doc) for doc in docs}
    result: List[LibraryItem] = []
    for entry in entries:
        series_asin = entry.get("series_asin")
        if series_asin and series_map.get(series_asin):
            result.append(_library_item_from_series_payload(series_map[series_asin], entry, username))
            continue
        result.append(_manual_library_item(entry, username))
    return result


async def add_to_library(username: str, item: LibraryItem, skip_fetch: bool = False) -> LibraryItem:
    user_col = get_user_library_collection()
    query: Dict[str, Any] = {"username": username}
    if item.asin:
        query["series_asin"] = item.asin
    else:
        query["title"] = item.title
    existing = user_col.find_one(query)
    if existing:
        series_asin = existing.get("series_asin")
        if series_asin:
            doc = get_series_collection().find_one({"_id": series_asin})
            if doc:
                return _library_item_from_series_payload(_series_payload(doc), existing, username)
        return _manual_library_item(existing, username)

    entry: Dict[str, Any] = {
        "username": username,
        "title": item.title,
        "added_at": _now_iso(),
    }
    if item.url:
        entry["url"] = item.url
    if item.asin:
        entry["series_asin"] = item.asin
        series_payload = None
        product = None

        if not skip_fetch:
            # Check if series has placeholder issue_date before adding
            from lib.audible_api_search import get_product_by_asin
            from .settings import load_settings
            settings = load_settings()
            proxies = _build_proxies(settings)
            try:
                resp = await get_product_by_asin(item.asin, auth_token=None, proxies=proxies, user_agent=settings.user_agent)
                product = resp.get("product") if isinstance(resp, dict) and "product" in resp else resp
                if isinstance(product, dict) and product.get("issue_date") == "2200-01-01":
                    from fastapi import HTTPException
                    raise HTTPException(status_code=400, detail="Cannot add series with placeholder issue_date")
            except HTTPException:
                raise
            except Exception:
                pass  # If fetch fails, continue with add

        # Ensure series doc exists (title/url may be enough when skip_fetch is true)
        series_payload, _ = ensure_series_document(item.asin, item.title, item.url)
        # Save raw product data if we successfully fetched it
        if isinstance(product, dict):
            set_series_raw(item.asin, product)
        user_col.insert_one(entry)
        _increment_series_user_count(item.asin, 1)
        return _library_item_from_series_payload(series_payload, entry, username)

    user_col.insert_one(entry)
    return _manual_library_item(entry, username)


def remove_from_library(username: str, asin: Optional[str] = None, title: Optional[str] = None) -> Dict[str, int]:
    if not asin and not title:
        return {"deleted": 0}
    query: Dict[str, Any] = {"username": username}
    if asin:
        query["series_asin"] = asin
    if title and not asin:
        query["title"] = title
    result = get_user_library_collection().delete_one(query)
    if result.deleted_count and asin:
        _increment_series_user_count(asin, -1)
    return {"deleted": result.deleted_count}


def _increment_series_user_count(asin: Optional[str], delta: int) -> None:
    if not asin:
        return
    series_col = get_series_collection()
    series_col.update_one({"_id": asin}, {"$inc": {"user_count": delta}})
    if delta < 0:
        series_col.update_one({"_id": asin, "user_count": {"$lt": 0}}, {"$set": {"user_count": 0}})


def ensure_indexes() -> None:
    user_col = get_user_library_collection()
    user_col.create_index([("username", ASCENDING)])
    user_col.create_index([("series_asin", ASCENDING)])
    series_col = get_series_collection()
    series_col.create_index([("title", ASCENDING)])
    series_col.create_index([("user_count", ASCENDING)])
    # Additional indexes for performance
    from .db import get_logs_collection, get_jobs_collection, get_users_collection
    logs_col = get_logs_collection()
    logs_col.create_index([("timestamp", ASCENDING)])
    jobs_col = get_jobs_collection()
    jobs_col.create_index([("created_at", ASCENDING)])
    jobs_col.create_index([("status", ASCENDING)])
    users_col = get_users_collection()
    users_col.create_index([("username", ASCENDING)], unique=True)
    users_col.create_index([("frontpage_slug", ASCENDING)], unique=True, sparse=True)


def rebuild_series_user_counts() -> None:
    user_col = get_user_library_collection()
    pipeline = [
        {"$match": {"series_asin": {"$exists": True}}},
        {"$group": {"_id": "$series_asin", "count": {"$sum": 1}}},
    ]
    series_col = get_series_collection()
    counts = {doc["_id"]: doc.get("count", 0) for doc in user_col.aggregate(pipeline)}
    ops = [UpdateOne({"_id": asin}, {"$set": {"user_count": count}}) for asin, count in counts.items()]
    if ops:
        series_col.bulk_write(ops, ordered=False)
    series_col.update_many({"user_count": {"$exists": False}}, {"$set": {"user_count": 0}})


def _extract_products(response: Any) -> List[Dict[str, Any]]:
    if isinstance(response, dict):
        for candidate in ("products", "Items", "items", "search_results", "SearchResults"):
            if candidate in response and isinstance(response[candidate], list):
                return response[candidate]
    return []


def _format_series_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/pd/"):
        url = url.replace("/pd/", "/series/", 1)
    if not url.startswith("/"):
        url = "/" + url
    return "https://www.audible.com" + url


def _clean_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return None


def _build_proxies(settings) -> Optional[Dict[str, str]]:
    # If proxy_enabled is not True, do not build proxies. Default is disabled.
    if not getattr(settings, "proxy_enabled", False):
        return None
    if not settings.proxy_url:
        return None
    proxy = settings.proxy_url
    if settings.proxy_username and settings.proxy_password and "@" not in proxy:
        # Insert auth into proxy URL
        proto_split = proxy.split("://", 1)
        if len(proto_split) == 2:
            proxy = f"{proto_split[0]}://{settings.proxy_username}:{settings.proxy_password}@{proto_split[1]}"
    return {
        "http://": proxy,
        "https://": proxy,
    }


def _book_summary(product: Dict[str, Any]) -> Dict[str, Any]:
    narrators = [n.get("name") for n in product.get("narrators", []) if isinstance(n, dict)]
    images = product.get("product_images", {})
    image = None
    if isinstance(images, dict) and images:
        image = next(iter(images.values()))
    return {
        "title": product.get("title") or product.get("publication_name") or product.get("product_title") or "Unknown",
        "asin": product.get("asin"),
        "url": _clean_url(_format_series_url(product.get("url"))) if product.get("url") else None,
        "release_date": product.get("release_date"),
        "runtime": product.get("runtime_length_min"),
        "narrators": ", ".join([n for n in narrators if n]),
        "image": image,
        "raw": product,
    }


def fetch_series_books(series_asin: str, response_groups: Optional[str], marketplace: Optional[str]) -> List[Dict[str, Any]]:
    """Fetch series books and return book list. Parent product is NOT returned; use _fetch_series_books_internal when raw data is needed."""
    books, _, _ = _fetch_series_books_internal(series_asin, response_groups, marketplace)
    return books


def _fetch_series_books_internal(series_asin: str, response_groups: Optional[str], marketplace: Optional[str]) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
    """Internal function that returns books, parent product object, and parent asin (if different)."""
    from .settings import load_settings
    settings = load_settings()
    if settings.debug_logging:
        logging.info(f"Starting fetch for series {series_asin}")
    proxies = _build_proxies(settings)
    rg = response_groups or settings.response_groups or DEFAULT_RESPONSE_GROUPS

    async def _load_product(asin: str) -> Optional[Dict[str, Any]]:
        try:
            if settings.debug_logging:
                logging.info(f"Fetching product {asin}")
            resp = await get_product_by_asin(asin, response_groups=rg, auth_token=None, marketplace=marketplace, proxies=proxies, user_agent=settings.user_agent if settings else None)
        except Exception as e:
            if settings.debug_logging:
                logging.error(f"Failed to fetch product {asin}: {e}")
            return None
        if not isinstance(resp, dict):
            if settings.debug_logging:
                logging.warning(f"Invalid response for {asin}: {type(resp)}")
            return None
        product = resp.get("product") if "product" in resp else resp
        if settings.debug_logging:
            logging.info(f"Fetched product {asin}: {bool(product)}")
        return product

    series_obj = asyncio.run(_load_product(series_asin))
    if not series_obj:
        if settings.debug_logging:
            logging.warning(f"No series object for {series_asin}")
        return [], None, None

    parent_asin = None
    for rel in series_obj.get("relationships", []):
        if isinstance(rel, dict) and rel.get("relationship_type") == "series" and rel.get("relationship_to_product") == "parent" and rel.get("asin"):
            parent_asin = rel.get("asin")
            break
    # If the provided ASIN already represents a series, treat it as the parent
    if not parent_asin and (series_obj.get("content_delivery_type") == "BookSeries" or any(isinstance(r, dict) and r.get("relationship_to_product") == "child" for r in series_obj.get("relationships", []))):
        parent_asin = series_asin

    parent_obj = series_obj if not parent_asin or parent_asin == series_asin else asyncio.run(_load_product(parent_asin))
    if not parent_obj:
        if settings.debug_logging:
            logging.warning(f"No parent object for {series_asin}, parent_asin: {parent_asin}")
        return [], None, None

    child_entries: List[Dict[str, Any]] = []
    for rel in parent_obj.get("relationships", []):
        if not isinstance(rel, dict):
            continue
        if rel.get("relationship_to_product") in ("child",) or rel.get("relationship_type") in ("component", "series"):
            asin_val = rel.get("asin")
            if asin_val:
                child_entries.append({"asin": asin_val, "rel": rel})

    def _sort_key(entry: Dict[str, Any]):
        rel = entry.get("rel", {})
        seq = rel.get("sequence") or rel.get("sort")
        try:
            return int(seq)
        except Exception:
            return 0

    child_entries.sort(key=_sort_key)

    books: List[Dict[str, Any]] = []
    for entry in child_entries:
        child_asin = entry.get("asin")
        rel = entry.get("rel", {}) or {}
        if not child_asin:
            continue
        child_obj = asyncio.run(_load_product(child_asin))
        if not child_obj:
            continue
        # Skip books with placeholder issue_date
        if child_obj.get("issue_date") == "2200-01-01":
            continue
        book = _book_summary(child_obj)
        if not book.get("asin"):
            book["asin"] = child_asin
        # Attach series relationship info so sequence can be detected later
        try:
            seq = rel.get("sequence") if isinstance(rel, dict) else None
            if not seq:
                seq = rel.get("sort") if isinstance(rel, dict) else None
        except Exception:
            seq = None
        # Use parent_asin (determined earlier) as the series ASIN for relationship entries
        primary_series_asin = parent_asin or series_asin
        book["series"] = [{"asin": primary_series_asin, "sequence": seq}] if primary_series_asin else []
        # fetch image data and store
        try:
            img_resp = requests.get(book["image"], timeout=10, proxies=proxies)
            if img_resp.ok:
                encoded = base64.b64encode(img_resp.content).decode("ascii")
                ctype = img_resp.headers.get("Content-Type", "image/jpeg")
                book["image"] = f"data:{ctype};base64,{encoded}"
            else:
                book["image"] = None
        except Exception:
            book["image"] = None
        books.append(book)

    filtered_books: List[Dict[str, Any]] = books

    if settings.debug_logging:
        logging.info(f"Fetched {len(filtered_books)} books for series {series_asin}")
    return filtered_books, parent_obj if isinstance(parent_obj, dict) else None, parent_asin or series_asin


def rebuild_series_user_counts():
    """Rebuild user counts for all series based on current library entries."""
    series_col = get_series_collection()
    lib_col = get_user_library_collection()
    
    # Reset all to 0
    series_col.update_many({}, {"$set": {"user_count": 0}})
    
    # Count from library
    counts = {}
    for doc in lib_col.aggregate([
        {"$match": {"series_asin": {"$exists": True}}},
        {"$group": {"_id": "$series_asin", "cnt": {"$sum": 1}}}
    ]):
        counts[doc["_id"]] = doc.get("cnt", 0)
    
    # Update counts
    for asin, cnt in counts.items():
        series_col.update_one({"_id": asin}, {"$set": {"user_count": cnt}})


def ensure_indexes():
    """Ensure necessary indexes exist."""
    series_col = get_series_collection()
    lib_col = get_user_library_collection()
    users_col = get_users_collection()
    jobs_col = get_jobs_collection()
    
    # Series indexes
    series_col.create_index("title")
    series_col.create_index("next_refresh_at")
    
    # Library indexes
    lib_col.create_index("username")
    lib_col.create_index("series_asin")
    lib_col.create_index([("username", ASCENDING), ("series_asin", ASCENDING)])
    
    # Users indexes
    users_col.create_index("username", unique=True)
    users_col.create_index("frontpage_slug", unique=True, sparse=True)
    
    # Jobs indexes