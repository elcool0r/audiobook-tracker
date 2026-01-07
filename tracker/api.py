from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, constr, Field
from typing import Dict, Any, List
import re
import logging
import copy
from math import ceil

from .auth import get_current_user
from .db import get_users_collection, get_user_library_collection, get_series_collection, get_jobs_collection
from .settings import load_settings, save_settings, Settings
from .library import (
    get_user_library,
    add_to_library,
    remove_from_library,
    LibraryItem,
    fetch_series_books,
    set_series_books,
    set_series_raw,
    _fetch_series_books_internal,
    _clean_url,
    _build_proxies,
    visible_books,
    visible_book_count,
    compute_narrator_warnings,
)
from .tasks import enqueue_fetch_series_books, enqueue_test_job, enqueue_delete_series, enqueue_refresh_probe, worker
from lib.audible_api_search import search_audible, get_product_by_asin, set_rate, DEFAULT_RESPONSE_GROUPS

api_router = APIRouter()

logger = logging.getLogger(__name__)

SKIP_HEADER = 'x-audiobook-search-skipped'
SKIP_REASON_HEADER = 'x-audiobook-search-skipped-reason'
SKIP_TITLE_HEADER = 'x-audiobook-search-skipped-title'
SKIP_ASIN_HEADER = 'x-audiobook-search-skipped-asin'
SKIP_COUNT_HEADER = 'x-audiobook-search-skipped-count'
SKIP_REASON_KNOWN = 'known_series_title'


def _utcnow_iso() -> str:
    iso = datetime.now(timezone.utc).isoformat()
    return iso.replace('+00:00', 'Z')


class SearchRequest(BaseModel):
    title: str
    num_results: int | None = None


class SeriesSearchRequest(BaseModel):
    query: str
    num_results: int | None = None


class SeriesResult(BaseModel):
    title: str
    asin: str | None = None
    url: str | None = None
    cover_url: str | None = None


class LibraryAddRequest(BaseModel):
    title: str
    asin: str | None = None
    url: str | None = None
    skip_fetch: bool | None = None


class FrontpageSlugRequest(BaseModel):
    slug: str


class SettingsSaveRequest(BaseModel):
    rate_rps: float | None = None
    max_job_history: int | None = None
    auto_refresh_enabled: bool | None = None
    manual_refresh_interval_minutes: int | None = None
    response_groups: str | None = None
    secret_key: str | None = None
    user_agent: str | None = None
    allow_non_admin_series_search: bool | None = None
    skip_known_series_search: bool | None = None
    proxy_enabled: bool | None = None
    proxy_url: str | None = None
    proxy_username: str | None = None
    proxy_password: str | None = None
    default_frontpage_slug: str | None = None
    users_can_edit_frontpage_slug: bool | None = None
    debug_logging: bool | None = None
    developer_mode: bool | None = None
    google_analytics_id: str | None = None


class SeriesBookVisibilityRequest(BaseModel):
    book_asin: str | None = None
    title: str | None = None
    hidden: bool


class SeriesBookIgnoreNarratorRequest(BaseModel):
    book_asin: str | None = None
    title: str | None = None
    ignore_narrator_warning: bool


class SeriesTitleUpdateRequest(BaseModel):
    title: str


class SeriesIgnoreSeriesRequest(BaseModel):
    ignore: bool


class DeveloperSeriesBookActionRequest(BaseModel):
    book_asin: str | None = None
    title: str | None = None


class DeveloperSeriesBookDatetimeRequest(DeveloperSeriesBookActionRequest):
    publication_datetime: str | None = None


class DeveloperSeriesDuplicateRequest(BaseModel):
    target_asin: str


@api_router.post("/search")
async def api_search(req: SearchRequest):
    settings = load_settings()
    if req.num_results is None:
        num_results = settings.default_num_results
    else:
        num_results = req.num_results
    # apply rate setting
    set_rate(settings.rate_rps)
    logger.info(f"API search request: title='{req.title}', num_results={num_results}")
    try:
        response_groups = settings.response_groups or DEFAULT_RESPONSE_GROUPS
        proxies = _build_proxies(settings)
        result = await search_audible(req.title, num_results=num_results, response_groups=response_groups, auth_token=None, proxies=proxies, user_agent=settings.user_agent)
        logger.info(f"API search successful: title='{req.title}', returned {len(result.get('products', [])) if isinstance(result, dict) else 'unknown'} products")
        return result
    except Exception as exc:
        logger.error(f"API search failed: title='{req.title}', error={str(exc)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc))


@api_router.get("/product/{asin}")
async def api_product(asin: str):
    settings = load_settings()
    set_rate(settings.rate_rps)
    logger.info(f"Product request: asin='{asin}'")
    try:
        proxies = _build_proxies(settings)
        result = await get_product_by_asin(asin, response_groups=settings.response_groups, auth_token=None, proxies=proxies, user_agent=settings.user_agent)
        logger.info(f"Product request successful: asin='{asin}'")
        return result
    except Exception as exc:
        logger.error(f"Product request failed: asin='{asin}', error={str(exc)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc))


@api_router.get("/settings")
async def api_get_settings(user=Depends(get_current_user)):
    return load_settings()

@api_router.post("/settings")
async def api_save_settings(payload: SettingsSaveRequest, user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    current = load_settings()
    provided_fields = payload.model_fields_set

    def _pick(field: str, current_value):
        # Respect explicit nulls so proxy fields can be cleared when desired.
        return getattr(payload, field) if field in provided_fields else current_value

    slug_candidate = (payload.default_frontpage_slug or "").strip()
    slug = None
    if slug_candidate:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", slug_candidate):
            raise HTTPException(status_code=400, detail="Slug may contain letters, numbers, hyphen, underscore")
        if not get_users_collection().find_one({"$or": [{"frontpage_slug": slug_candidate}, {"username": slug_candidate}]}):
            raise HTTPException(status_code=400, detail="User not found for provided slug")
        slug = slug_candidate
    updated = Settings(
        rate_rps=payload.rate_rps if payload.rate_rps is not None else current.rate_rps,
        response_groups=payload.response_groups if payload.response_groups is not None else current.response_groups,
        secret_key=payload.secret_key if payload.secret_key is not None else current.secret_key,
        proxy_enabled=_pick("proxy_enabled", current.proxy_enabled),
        proxy_url=_pick("proxy_url", current.proxy_url),
        proxy_username=_pick("proxy_username", current.proxy_username),
        proxy_password=_pick("proxy_password", current.proxy_password),
        max_job_history=payload.max_job_history if payload.max_job_history is not None else current.max_job_history,
        auto_refresh_enabled=payload.auto_refresh_enabled if payload.auto_refresh_enabled is not None else current.auto_refresh_enabled,
        manual_refresh_interval_minutes=payload.manual_refresh_interval_minutes if payload.manual_refresh_interval_minutes is not None else current.manual_refresh_interval_minutes,
        user_agent=payload.user_agent if payload.user_agent is not None else current.user_agent,
        allow_non_admin_series_search=payload.allow_non_admin_series_search if payload.allow_non_admin_series_search is not None else current.allow_non_admin_series_search,
        skip_known_series_search=payload.skip_known_series_search if payload.skip_known_series_search is not None else current.skip_known_series_search,
        default_frontpage_slug=slug,
        users_can_edit_frontpage_slug=payload.users_can_edit_frontpage_slug if payload.users_can_edit_frontpage_slug is not None else current.users_can_edit_frontpage_slug,
        debug_logging=payload.debug_logging if payload.debug_logging is not None else current.debug_logging,
        developer_mode=payload.developer_mode if payload.developer_mode is not None else current.developer_mode,
        google_analytics_id=payload.google_analytics_id if payload.google_analytics_id is not None else current.google_analytics_id,
    )
    save_settings(updated)
    try:
        if not current.auto_refresh_enabled and updated.auto_refresh_enabled:
            # Setting toggled on: rebalance all series and ensure scheduler thread is running
            worker.ensure_scheduler_running(rebalance=True)
    except Exception:
        pass
    return updated

@api_router.post("/settings/test-proxy")
async def api_test_proxy(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    
    settings = load_settings()
    if not getattr(settings, "proxy_enabled", True):
        return {"success": False, "error": "Proxy is disabled"}
    if not settings.proxy_url:
        return {"success": False, "error": "Proxy URL is not configured"}
    proxies = _build_proxies(settings)
    import requests
    try:
        response = requests.get("https://www.audible.com", proxies=proxies, timeout=10)
        if response.status_code == 200:
            return {"success": True, "message": f"Proxy connection successful (status: {response.status_code})"}
        return {"success": False, "error": f"Unexpected status code: {response.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- Series search helpers ---

def _extract_series_key_and_titles(product: Dict[str, Any]) -> tuple:
    titles: list = []
    keys: list = []
    urls: list = []
    asins: list = []

    def _extract_image_value(val: Any):
        if not val:
            return None
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            for candidate in ("url", "image", "src", "href", "large", "detail", "medium", "small", "thumbnail"):
                nested = val.get(candidate)
                img = _extract_image_value(nested)
                if img:
                    return img
            for nested in val.values():
                img = _extract_image_value(nested)
                if img:
                    return img
            return None
        if isinstance(val, list):
            for nested in val:
                img = _extract_image_value(nested)
                if img:
                    return img
        return None

    s = product.get("series")

    def _collect_from_obj(obj: Dict[str, Any]):
        for k in ("id", "asin", "series_id", "seriesAsin", "series_asin", "product_series_id"):
            if k in obj and obj.get(k):
                keys.append(str(obj.get(k)))
                break
        for k in ("title", "series_title", "name"):
            if k in obj and obj.get(k):
                titles.append(obj.get(k))
                break
        if obj.get("url"):
            urls.append(obj.get("url"))
        if obj.get("asin"):
            asins.append(str(obj.get("asin")))

    if isinstance(s, dict):
        _collect_from_obj(s)
    elif isinstance(s, list):
        for e in s:
            if isinstance(e, dict):
                _collect_from_obj(e)
            elif isinstance(e, str):
                titles.append(e)

    for rel in product.get("relationships", []):
        if not isinstance(rel, dict):
            continue
        if rel.get("relationship_type") == "series" or rel.get("relationship_to_product") == "parent":
            if rel.get("asin"):
                keys.append(str(rel.get("asin")))
            if rel.get("title"):
                titles.append(rel.get("title"))
            if rel.get("url"):
                urls.append(rel.get("url"))
            if rel.get("asin"):
                asins.append(str(rel.get("asin")))

    for key in ("series_id", "product_series_id", "seriesAsin", "series_asin"):
        if key in product and product.get(key):
            keys.append(str(product.get(key)))
    for key in ("series_title", "product_series_title"):
        if key in product and isinstance(product[key], str):
            titles.append(product[key])

    if product.get("url"):
        urls.append(product.get("url"))
    if product.get("asin"):
        asins.append(str(product.get("asin")))

    seen = set()
    uniq_titles = []
    for t in titles:
        if t and t not in seen:
            seen.add(t)
            uniq_titles.append(t)
    seen_u = set()
    uniq_urls = []
    for u in urls:
        if u and u not in seen_u:
            seen_u.add(u)
            uniq_urls.append(u)
    seen_a = set()
    uniq_asins = []
    for a in asins:
        if a and a not in seen_a:
            seen_a.add(a)
            uniq_asins.append(a)

    key = keys[0] if keys else None
    image = None
    for candidate in (
        product.get("product_images"),
        product.get("image"),
        product.get("cover"),
        product.get("cover_image"),
        product.get("product_image"),
    ):
        image = _extract_image_value(candidate)
        if image:
            break
    return key, uniq_titles, uniq_urls, uniq_asins, image


def _format_series_url(url: str) -> str:
    if not url:
        return url
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/pd/"):
        url = url.replace("/pd/", "/series/", 1)
    if not url.startswith("/"):
        url = "/" + url
    return "https://www.audible.com" + url


def _find_series_by_exact_title(query: str) -> Dict[str, Any] | None:
    trimmed = (query or "").strip()
    if not trimmed:
        return None
    regex = re.compile(r"^" + re.escape(trimmed) + r"$", re.IGNORECASE)
    col = get_series_collection()
    return col.find_one({
        "$or": [
            {"title": regex},
            {"original_title": regex},
        ]
    })


@api_router.post("/series/search")
async def api_series_search(req: SeriesSearchRequest, user=Depends(get_current_user)):
    settings = load_settings()
    user_role = getattr(user, "role", None) if not isinstance(user, dict) else user.get("role")
    if (not user or user_role != "admin") and not settings.allow_non_admin_series_search:
        raise HTTPException(status_code=403, detail="Series search disabled for non-admin users")
    search_query = (req.query or "").strip()
    if settings.skip_known_series_search:
        matched = _find_series_by_exact_title(search_query)
        if matched:
            matched_title = (matched.get("title") or matched.get("original_title") or search_query).strip()
            logger.info(f"Series search skipped (known title) query='{search_query}' user={user.get('username') if isinstance(user, dict) else getattr(user, 'username', 'unknown')} title='{matched_title}' asin='{matched.get('_id') or ''}'")
            headers = {
                SKIP_HEADER: "true",
                SKIP_REASON_HEADER: SKIP_REASON_KNOWN,
                SKIP_TITLE_HEADER: matched_title,
                SKIP_ASIN_HEADER: str(matched.get("_id") or ""),
                SKIP_COUNT_HEADER: "1",
            }
            return JSONResponse(content=[], headers=headers)
    set_rate(settings.rate_rps)
    num_results = req.num_results or 10
    logger.info(f"Series search request: query='{search_query}', num_results={num_results}, user={user.get('username') if isinstance(user, dict) else getattr(user, 'username', 'unknown')}")
    try:
        response_groups = settings.response_groups or DEFAULT_RESPONSE_GROUPS
        proxies = _build_proxies(settings)
        response = await search_audible(search_query, num_results=num_results, response_groups=response_groups, auth_token=None, proxies=proxies, user_agent=settings.user_agent)
        logger.info(f"Series search successful: query='{search_query}', found {len(response.get('products', [])) if isinstance(response, dict) else 'unknown'} products")
    except Exception as exc:
        logger.error(f"Series search failed: query='{req.query}', error={str(exc)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc))

    products: List[Dict[str, Any]] | None = None
    if isinstance(response, dict):
        for candidate in ("products", "Items", "items", "search_results", "SearchResults"):
            if candidate in response and isinstance(response[candidate], list):
                products = response[candidate]
                break
    if not products:
        return []

    groups: Dict[str, Dict[str, object]] = {}
    for p in products:
        key, titles, urls, asins, cover_url = _extract_series_key_and_titles(p)
        group_key = key or "__no_series_key__"
        entry = groups.setdefault(group_key, {"count": 0, "titles": {}, "cover_url": None})
        entry["count"] = int(entry.get("count", 0)) + 1
        if cover_url and not entry.get("cover_url"):
            entry["cover_url"] = cover_url
        for t in titles:
            entry["titles"].setdefault(t, {"urls": set(), "asins": set()})
            entry["titles"][t]["urls"].update(urls)
            entry["titles"][t]["asins"].update(asins)

    results: List[SeriesResult] = []
    SERIES_KEY_SENTINEL = "__no_series_key__"
    for group_key, entry in sorted(groups.items(), key=lambda kv: kv[1]["count"], reverse=True):
        title_map = entry.get("titles", {})
        for t, data in title_map.items():
            urls_for_t = sorted(_format_series_url(u) for u in data.get("urls", []))
            asins_for_t = sorted(data.get("asins", []))
            keys_for_group = set(entry.get("titles", {}).get(t, {}).get("asins", []))
            series_asin = None
            # Prefer the series group key when available (represents the parent series).
            if group_key != SERIES_KEY_SENTINEL:
                series_asin = group_key
            else:
                # Otherwise pick an ASIN that appears as a series key, falling back to first.
                for cand in asins_for_t:
                    if cand in keys_for_group:
                        series_asin = cand
                        break
                if not series_asin and asins_for_t:
                    series_asin = asins_for_t[0]
            results.append(
                SeriesResult(
                    title=t,
                    asin=series_asin,
                    url=_clean_url(urls_for_t[0]) if urls_for_t else None,
                    cover_url=entry.get("cover_url"),
                )
            )
    return results


# --- Library endpoints ---


@api_router.get("/library")
async def api_get_library(user=Depends(get_current_user)):
    # Return the current user's library entries only
    library = get_user_library(user["username"])
    return jsonable_encoder(library)


@api_router.post("/library")
async def api_add_library(payload: LibraryAddRequest, user=Depends(get_current_user)):
    title = (payload.title or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title is required")

    item = LibraryItem(title=title, asin=payload.asin, url=_clean_url(payload.url))
    skip_fetch = bool(payload.skip_fetch)
    added = await add_to_library(user["username"], item, skip_fetch=True)  # Always skip fetch for speed

    job_id = None
    if added.asin:
        try:
            job_id = enqueue_fetch_series_books(user["username"], added.asin)
        except Exception:
            job_id = None

    encoded = jsonable_encoder(added)
    return {"item": encoded, "job_id": job_id} if job_id else encoded


@api_router.delete("/library")
async def api_delete_library(asin: str | None = None, title: str | None = None, user=Depends(get_current_user)):
    updated = remove_from_library(user["username"], asin=asin, title=title)
    return updated


# --- Public frontpage (read-only) ---


@api_router.get("/public/home/{slug}")
async def api_public_frontpage(slug: str):
    users_col = get_users_collection()
    # Allow fallback to username if frontpage_slug is not set
    user_doc = users_col.find_one({"$or": [{"frontpage_slug": slug}, {"username": slug}]})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    username = user_doc.get("username")
    library = get_user_library(username)
    return {
        "user": {
            "username": username,
            "date_format": user_doc.get("date_format", "de"),
            "frontpage_slug": user_doc.get("frontpage_slug") or username,
        },
        "library": library,
    }


@api_router.get("/public/series/{asin}")
async def api_public_series_info(asin: str):
    from .library import get_series_document
    series = get_series_document(asin)
    if series and series.get("books"):
        return series
    # Fallback: fetch books and cache
    settings = load_settings()
    response_groups = settings.response_groups or DEFAULT_RESPONSE_GROUPS
    books, parent_obj, parent_asin = _fetch_series_books_internal(asin, response_groups, None)
    if books:
        target_asin = parent_asin or asin
        set_series_books(target_asin, books)
        if isinstance(parent_obj, dict):
            set_series_raw(target_asin, parent_obj)
            if parent_asin and parent_asin != asin:
                set_series_raw(parent_asin, parent_obj)
        updated = get_series_document(target_asin)
        if updated:
            return updated
    if series:
        return series
    raise HTTPException(status_code=404, detail="Series not found")


@api_router.get("/public/series/{asin}/books")
async def api_public_series_books(asin: str):
    from .library import get_series_document
    series = get_series_document(asin)
    if series and series.get("books"):
        return series.get("books")
    settings = load_settings()
    response_groups = settings.response_groups or DEFAULT_RESPONSE_GROUPS
    books, parent_obj, parent_asin = _fetch_series_books_internal(asin, response_groups, None)
    if books:
        target_asin = parent_asin or asin
        set_series_books(target_asin, books)
        if isinstance(parent_obj, dict):
            set_series_raw(target_asin, parent_obj)
            if parent_asin and parent_asin != asin:
                set_series_raw(parent_asin, parent_obj)
    return books


@api_router.get("/series/books/{asin}")
async def api_series_books(asin: str, user=Depends(get_current_user)):
    settings = load_settings()
    response_groups = settings.response_groups or DEFAULT_RESPONSE_GROUPS
    books, parent_obj, parent_asin = _fetch_series_books_internal(asin, response_groups, None)
    if isinstance(parent_obj, dict):
        target_asin = parent_asin or asin
        set_series_raw(target_asin, parent_obj)
        if parent_asin and parent_asin != asin:
            set_series_raw(parent_asin, parent_obj)
    target_asin = parent_asin or asin
    set_series_books(target_asin, books)
    return books


@api_router.get("/series/info/{asin}")
async def api_series_info(asin: str, user=Depends(get_current_user)):
    """Get series info from database by ASIN"""
    from .library import get_series_document
    series = get_series_document(asin)
    if not series:
        raise HTTPException(status_code=404, detail="Series not found")
    return series


@api_router.post("/series/{asin}/refresh")
async def api_series_refresh(asin: str, user=Depends(get_current_user)):
    """Manually enqueue a refresh probe for a series (admin only)."""
    _require_admin(user)
    job_id = enqueue_refresh_probe(asin, response_groups=None, source="manual")
    return {"job_id": job_id}


@api_router.put("/series/{asin}/title")
async def api_update_series_title(asin: str, payload: SeriesTitleUpdateRequest, user=Depends(get_current_user)):
    _require_admin(user)
    new_title = (payload.title or "").strip()
    if not new_title:
        raise HTTPException(status_code=400, detail="Title is required")
    series_col = get_series_collection()
    series_doc = series_col.find_one({"_id": asin})
    if not series_doc:
        raise HTTPException(status_code=404, detail="Series not found")
    updates: Dict[str, Any] = {
        "title": new_title,
        "updated_at": _utcnow_iso(),
    }
    existing_title = series_doc.get("title")
    if existing_title and existing_title != new_title and not series_doc.get("original_title"):
        updates["original_title"] = existing_title
    result = series_col.update_one({"_id": asin}, {"$set": updates})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Series not found")
    return {"asin": asin, "title": new_title}


@api_router.post("/series/{asin}/books/visibility")
async def api_series_book_visibility(asin: str, payload: SeriesBookVisibilityRequest, user=Depends(get_current_user)):
    _require_admin(user)
    if not payload.book_asin and not payload.title:
        raise HTTPException(status_code=400, detail="Book identifier required")
    series_col = get_series_collection()
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
    matched = False
    updated = False
    for book in books:
        if not isinstance(book, dict):
            continue
        match = False
        if payload.book_asin and book.get("asin") == payload.book_asin:
            match = True
        elif not payload.book_asin and payload.title and isinstance(book.get("title"), str) and book.get("title") == payload.title:
            match = True
        if not match:
            continue
        matched = True
        desired_hidden = bool(payload.hidden)
        if book.get("hidden") == desired_hidden:
            continue
        book["hidden"] = desired_hidden
        updated = True
    if updated:
        cover_image = None
        for book in books:
            if book.get("hidden"):
                continue
            if book.get("image"):
                cover_image = book.get("image")
                break
        series_col.update_one({"_id": asin}, {"$set": {"books": books, "cover_image": cover_image}})
    return {"matched": matched, "hidden": payload.hidden}


@api_router.post("/series/{asin}/ignore-narrator-series")
async def api_series_ignore_narrator_series(asin: str, payload: SeriesIgnoreSeriesRequest, user=Depends(get_current_user)):
    """Toggle series-level ignore for narrator changes. When enabled, existing books are marked ignored and narrator_warnings cleared; when disabled, series-set ignores are cleared and warnings recomputed."""
    _require_admin(user)
    series_col = get_series_collection()
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
    if payload.ignore:
        # Set series-level flag and mark existing books as ignored (remember which were set by series toggle)
        for b in books:
            if not isinstance(b, dict):
                continue
            b["ignore_narrator_warning"] = True
            b["ignore_narrator_warning_set_by_series"] = True
        series_col.update_one({"_id": asin}, {"$set": {"ignore_narrator_warnings": True, "books": books, "narrator_warnings": []}})
        return {"asin": asin, "ignore_narrator_warnings": True}
    else:
        # Clear series-level flag and revert book-level ignores that were set by the series toggle
        for b in books:
            if not isinstance(b, dict):
                continue
            if b.get("ignore_narrator_warning_set_by_series"):
                b["ignore_narrator_warning"] = False
                b.pop("ignore_narrator_warning_set_by_series", None)
        narrator_warnings = compute_narrator_warnings(books, asin)
        series_col.update_one({"_id": asin}, {"$set": {"ignore_narrator_warnings": False, "books": books, "narrator_warnings": narrator_warnings}})
        return {"asin": asin, "ignore_narrator_warnings": False, "narrator_warnings": narrator_warnings}

@api_router.post("/series/{asin}/books/ignore-narrator")
async def api_series_book_ignore_narrator(asin: str, payload: SeriesBookIgnoreNarratorRequest, user=Depends(get_current_user)):
    _require_admin(user)
    if not payload.book_asin and not payload.title:
        raise HTTPException(status_code=400, detail="Book identifier required")
    series_col = get_series_collection()
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
    matched = False
    updated = False
    for book in books:
        if not isinstance(book, dict):
            continue
        match = False
        if payload.book_asin and book.get("asin") == payload.book_asin:
            match = True
        elif not payload.book_asin and payload.title and isinstance(book.get("title"), str) and book.get("title") == payload.title:
            match = True
        if not match:
            continue
        matched = True
        desired_ignore = bool(payload.ignore_narrator_warning)
        if book.get("ignore_narrator_warning") == desired_ignore:
            continue
        # Update the book's ignore flag and clear any series-set marker if a user manually changes it
        book["ignore_narrator_warning"] = desired_ignore
        if not desired_ignore and book.get("ignore_narrator_warning_set_by_series"):
            book.pop("ignore_narrator_warning_set_by_series", None)
        updated = True
    if updated:
        narrator_warnings = compute_narrator_warnings(books, asin)
        series_col.update_one({"_id": asin}, {"$set": {"books": books, "narrator_warnings": narrator_warnings}})
        return {"matched": matched, "ignore_narrator_warning": payload.ignore_narrator_warning, "narrator_warnings": narrator_warnings}
    return {"matched": matched, "ignore_narrator_warning": payload.ignore_narrator_warning}


def _clear_release_notification_history(series_asin: str, book_asin: str | None):
    if not book_asin:
        return
    try:
        lib_col = get_user_library_collection()
        lib_col.update_many(
            {"series_asin": series_asin},
            {"$pull": {"notified_releases": book_asin}},
        )
    except Exception:
        pass


def _mark_new_asin_seen(series_asin: str, book_asin: str | None):
    if not book_asin:
        return
    try:
        lib_col = get_user_library_collection()
        lib_col.update_many(
            {"series_asin": series_asin},
            {
                "$addToSet": {"notified_new_asins": book_asin},
                "$set": {"notified_new_asins_initialized": True},
            },
        )
    except Exception:
        pass


def _clear_series_notification_history(series_asin: str, book_asin: str | None):
    if not book_asin:
        return
    _clear_release_notification_history(series_asin, book_asin)
    try:
        lib_col = get_user_library_collection()
        lib_col.update_many(
            {"series_asin": series_asin},
            {"$pull": {"notified_new_asins": book_asin}},
        )
    except Exception:
        pass


def _send_developer_notification_to_user(username: str | None, title: str, body: str, attaches: list[str] | None = None) -> bool:
    if not username:
        return False
    user_doc = get_users_collection().find_one({"username": username}) or {}
    notif = user_doc.get("notifications", {})
    urls = [u for u in (notif.get("urls") or []) if isinstance(u, str) and u.strip()]
    if not urls:
        return False
    try:
        import apprise
        ap = apprise.Apprise()
        for url in urls:
            ap.add(url)
        try:
            result = ap.notify(title=title, body=body)
            if not result:
                logger.warning("Developer notification to %s failed (apprise returned False)", username)
                return False
            return True
        except Exception as exc:
            logger.exception("Developer notification failed for %s", username)
            return False
    except Exception as exc:
        logger.exception("Developer notification failed to initialize apprise for %s: %s", username, exc)
        return False

@api_router.post("/developer/series/{asin}/books/mark-new")
async def api_developer_mark_book_new(asin: str, payload: DeveloperSeriesBookActionRequest, user=Depends(get_current_user)):
    _require_developer_mode(user)
    series_col = get_series_collection()
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
    matched_book = None
    old_books = []
    for book in books:
        if matched_book is None and _book_matches(book, payload.book_asin, payload.title):
            matched_book = book
            continue
        old_books.append(book)
    if not matched_book:
        raise HTTPException(status_code=404, detail="Book not found")
    if not old_books:
        old_books = [{"asin": "__dev_sentinel__"}]
    matched_book_asin = matched_book.get("asin") if isinstance(matched_book, dict) else None
    _clear_release_notification_history(asin, matched_book_asin)
    _mark_new_asin_seen(asin, matched_book_asin)
    release_job_recorded = worker._send_series_notifications(asin, doc, old_books, books)
    username = user.get("username") if isinstance(user, dict) else getattr(user, "username", None)
    if not release_job_recorded:
        series_title = doc.get("title") or doc.get("original_title") or f"Series {asin}"
        pending_asins = [matched_book_asin] if matched_book_asin else []
        body = f"Developer triggered notification for '{matched_book.get('title') or matched_book_asin or 'book'}' in '{series_title}'."
        worker._record_release_job(
            username=username,
            asin=asin,
            series_title=series_title,
            pending_asins=pending_asins,
            body=body,
            success=True,
            error=None,
        )
        image_url = None
        if isinstance(matched_book, dict):
            image_val = matched_book.get("image")
            if isinstance(image_val, str) and image_val:
                image_url = image_val
        _send_developer_notification_to_user(
            username,
            f"Developer notification for {series_title}",
            body,
            [image_url] if image_url else None,
        )
    return {"status": "notification_queued", "book_asin": matched_book.get("asin"), "book_title": matched_book.get("title")}


@api_router.post("/developer/series/{asin}/books/update-publication")
async def api_developer_update_publication_datetime(
    asin: str,
    payload: DeveloperSeriesBookDatetimeRequest,
    user=Depends(get_current_user),
):
    """Legacy endpoint: update the cached books list and trigger notifications. Kept for compatibility."""
    _require_developer_mode(user)
    if not payload.book_asin and not payload.title:
        raise HTTPException(status_code=400, detail="Book identifier required")
    series_col = get_series_collection()
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
    original_books = copy.deepcopy(books)
    updated = False
    matched_book = None
    new_books = []
    for book in books:
        if not updated and _book_matches(book, payload.book_asin, payload.title):
            new_book = dict(book)
            new_book["publication_datetime"] = payload.publication_datetime if payload.publication_datetime else None
            matched_book = new_book
            new_books.append(new_book)
            updated = True
            continue
        new_books.append(book)
    if not matched_book:
        raise HTTPException(status_code=404, detail="Book not found")
    matched_book_asin = matched_book.get("asin") if isinstance(matched_book, dict) else None
    cover_image = _select_cover_image(new_books)
    series_col.update_one({"_id": asin}, {"$set": {"books": new_books, "cover_image": cover_image}})
    doc["books"] = new_books
    release_job_recorded = worker._send_series_notifications(asin, doc, original_books, new_books)
    username = user.get("username") if isinstance(user, dict) else getattr(user, "username", None)
    if not release_job_recorded:
        series_title = doc.get("title") or doc.get("original_title") or f"Series {asin}"
        pending_asins = [matched_book_asin] if matched_book_asin else []
        publication_text = payload.publication_datetime or "<cleared>"
        body = f"Developer updated publication datetime for '{matched_book.get('title') or matched_book_asin or 'book'}' to '{publication_text}' in '{series_title}'."
        worker._record_release_job(
            username=username,
            asin=asin,
            series_title=series_title,
            pending_asins=pending_asins,
            body=body,
            success=True,
            error=None,
        )
        image_url = None
        if isinstance(matched_book, dict):
            image_val = matched_book.get("image")
            if isinstance(image_val, str) and image_val:
                image_url = image_val
        _send_developer_notification_to_user(
            username,
            f"Developer publication update for {series_title}",
            body,
            [image_url] if image_url else None,
        )
    return {
        "asin": asin,
        "book_asin": matched_book.get("asin"),
        "publication_datetime": matched_book.get("publication_datetime"),
        "book": matched_book,
    }


@api_router.post("/developer/series/{asin}/books/update-publication-raw")
async def api_developer_update_publication_datetime_raw(
    asin: str,
    payload: DeveloperSeriesBookDatetimeRequest,
    user=Depends(get_current_user),
):
    """Developer-only: update publication_datetime in the book's 'raw' dict only. Does NOT trigger notifications and is overwritten on next series refresh."""
    _require_developer_mode(user)
    if not payload.book_asin and not payload.title:
        raise HTTPException(status_code=400, detail="Book identifier required")
    series_col = get_series_collection()
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
    matched_book = None
    new_books = []
    for book in books:
        if matched_book is None and _book_matches(book, payload.book_asin, payload.title):
            new_book = dict(book)
            raw = dict(new_book.get("raw") or {})
            # If client sends null, set server-side UTC now (use current time)
            if payload.publication_datetime is None:
                dt = datetime.now(timezone.utc)
                raw["publication_datetime"] = dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            elif payload.publication_datetime:
                raw["publication_datetime"] = payload.publication_datetime
            else:
                raw.pop("publication_datetime", None)
            new_book["raw"] = raw
            matched_book = new_book
            new_books.append(new_book)
            continue
        new_books.append(book)
    if not matched_book:
        raise HTTPException(status_code=404, detail="Book not found")
    matched_book_asin = matched_book.get("asin") if isinstance(matched_book, dict) else None
    _clear_series_notification_history(asin, matched_book_asin)
    # Persist only the raw update; do NOT trigger notifications or send developer emails
    try:
        series_col.update_one({"_id": asin}, {"$set": {"books": new_books}})
    except Exception as exc:
        logger.exception("Failed to update raw publication datetime for %s: %s", asin, exc)
        raise HTTPException(status_code=500, detail="Failed to update database")
    return {"status": "ok", "book": matched_book}


@api_router.delete("/developer/series/{asin}/books")
async def api_developer_delete_series_book(
    asin: str,
    payload: DeveloperSeriesBookActionRequest,
    user=Depends(get_current_user),
):
    _require_developer_mode(user)
    if not payload.book_asin and not payload.title:
        raise HTTPException(status_code=400, detail="Book identifier required")
    series_col = get_series_collection()
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
    new_books = []
    removed = None
    for book in books:
        if removed is None and _book_matches(book, payload.book_asin, payload.title):
            removed = book
            continue
        new_books.append(book)
    if not removed:
        raise HTTPException(status_code=404, detail="Book not found")
    cover_image = _select_cover_image(new_books)
    series_col.update_one({"_id": asin}, {"$set": {"books": new_books, "cover_image": cover_image}})
    if isinstance(removed.get("asin"), str):
        lib_col = get_user_library_collection()
        lib_col.update_many(
            {"series_asin": asin},
            {
                "$pull": {
                    "notified_new_asins": removed.get("asin"),
                    "notified_releases": removed.get("asin"),
                }
            },
        )
    return {"deleted": True, "book_asin": removed.get("asin"), "title": removed.get("title")}


@api_router.post("/developer/series/{asin}/duplicate")
async def api_developer_duplicate_series(
    asin: str,
    payload: DeveloperSeriesDuplicateRequest,
    user=Depends(get_current_user),
):
    _require_developer_mode(user)
    target = (payload.target_asin or "").strip()
    if not target:
        raise HTTPException(status_code=400, detail="Target ASIN is required")
    if target == asin:
        raise HTTPException(status_code=400, detail="Target ASIN must differ from source")
    series_col = get_series_collection()
    if series_col.find_one({"_id": target}):
        raise HTTPException(status_code=400, detail="Target ASIN already exists")
    doc = series_col.find_one({"_id": asin})
    if not doc:
        raise HTTPException(status_code=404, detail="Series not found")
    book_list = doc.get("books")
    books = copy.deepcopy(book_list) if isinstance(book_list, list) else []
    source_title = doc.get("title") or doc.get("original_title") or ""
    duplicate_title = f"{source_title}-copy" if source_title else f"{target}-copy"
    original_title = doc.get("original_title") or doc.get("title")
    new_doc = {
        "_id": target,
        "title": duplicate_title,
        "url": doc.get("url"),
        "books": books,
        "cover_image": _select_cover_image(books) or doc.get("cover_image"),
        "fetched_at": doc.get("fetched_at"),
        "raw": copy.deepcopy(doc.get("raw")) if doc.get("raw") is not None else None,
        "next_refresh_at": None,
        "user_count": 0,
        "created_at": _utcnow_iso(),
        "original_title": original_title,
    }
    series_col.insert_one(new_doc)
    return {"status": "ok", "asin": target}


@api_router.post("/developer/series/{asin}/probe")
async def api_developer_schedule_probe(asin: str, user=Depends(get_current_user)):
    _require_developer_mode(user)
    job_id = enqueue_refresh_probe(asin, response_groups=None, source="developer")
    return {"job_id": job_id}


# --- Users endpoints (admin only) ---


class UserCreateRequest(BaseModel):
    username: str = Field(..., pattern=r'^[A-Za-z0-9_-]+$')
    password: str
    role: str | None = "user"
    date_format: str | None = None


class UserUpdateRequest(BaseModel):
    username: str | None = None
    password: str | None = None
    role: str | None = None
    date_format: str | None = None


class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str


class ProfileUpdateRequest(BaseModel):
    date_format: str
    show_narrator_warnings: bool = True
    latest_count: int = 4  # how many latest releases to show (1-24)
    hide_narrator_warnings_for_dramatized_adaptations: bool = False


class ApiKeyCreateRequest(BaseModel):
    description: str


class SeriesRefreshRequest(BaseModel):
    response_groups: str | None = None


class JobListResponse(BaseModel):
    id: str
    type: str
    status: str
    username: str | None = None
    asin: str | None = None
    result: dict | None = None
    created_at: str | None = None


def _require_admin(user):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")


def _require_developer_mode(user):
    _require_admin(user)
    settings = load_settings()
    if not getattr(settings, "developer_mode", False):
        raise HTTPException(status_code=403, detail="Developer mode disabled")


def _book_matches(book: Dict[str, Any] | None, book_asin: str | None, title: str | None) -> bool:
    if not isinstance(book, dict):
        return False
    if book_asin:
        return book.get("asin") == book_asin
    if title:
        if not isinstance(book.get("title"), str):
            return False
        return book.get("title").strip().lower() == title.strip().lower()
    return False


def _select_cover_image(books: list | None) -> str | None:
    if not isinstance(books, list):
        return None
    for book in books:
        if not isinstance(book, dict):
            continue
        if book.get("hidden"):
            continue
        image = book.get("image")
        if image:
            return image
    return None


@api_router.get("/users")
async def api_list_users(user=Depends(get_current_user)):
    _require_admin(user)
    users = list(get_users_collection().find({}, {"_id": 0}))
    counts = {}
    col = get_user_library_collection()
    for u in users:
        counts[u["username"]] = col.count_documents({"username": u["username"]})
    for u in users:
        u["library_count"] = counts.get(u["username"], 0)
    return users


@api_router.post("/users")
async def api_create_user(payload: UserCreateRequest, user=Depends(get_current_user)):
    _require_admin(user)
    col = get_users_collection()
    if col.find_one({"username": payload.username}):
        raise HTTPException(status_code=400, detail="User already exists")
    from .auth import get_password_hash

    doc = {
        "username": payload.username,
        "password_hash": get_password_hash(payload.password),
        "role": payload.role or "user",

        "date_format": payload.date_format or "iso",
    }
    col.insert_one(doc)
    return {"status": "ok"}


@api_router.put("/users/{username}")
async def api_update_user(username: str, payload: UserUpdateRequest, user=Depends(get_current_user)):
    _require_admin(user)
    col = get_users_collection()
    existing_user = col.find_one({"username": username})
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")
    if existing_user.get("role") == "admin" and payload.role and payload.role != "admin":
        raise HTTPException(status_code=400, detail="Cannot change admin role")
    update: Dict[str, Any] = {}
    if payload.username:
        if col.find_one({"username": payload.username}):
            raise HTTPException(status_code=400, detail="Username already exists")
        update["username"] = payload.username
    if payload.password:
        from .auth import get_password_hash

        update["password_hash"] = get_password_hash(payload.password)
    if payload.role:
        update["role"] = payload.role
    if payload.date_format:
        update["date_format"] = payload.date_format
    if not update:
        raise HTTPException(status_code=400, detail="No changes")
    res = col.update_one({"username": username}, {"$set": update})
    return {"status": "ok"}


@api_router.delete("/users/{username}")
async def api_delete_user(username: str, user=Depends(get_current_user)):
    _require_admin(user)
    if username == "admin":
        raise HTTPException(status_code=400, detail="Cannot delete default admin")
    col = get_users_collection()
    res = col.delete_one({"username": username})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"status": "ok"}


# --- Series admin (admin) ---


@api_router.get("/series")
async def api_list_series(
    user=Depends(get_current_user),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
    filter: str | None = Query(None),
    sort: str | None = Query(None),
    order: str | None = Query(None),
):
    _require_admin(user)
    col = get_series_collection()
    query = {}
    if filter:
        text = filter.strip()
        if text:
            regex = {"$regex": re.escape(text), "$options": "i"}
            query = {"$or": [{"title": regex}, {"original_title": regex}]}
    total = col.count_documents(query)
    max_page = max(1, ceil(total / page_size)) if total > 0 else 1
    page = min(page, max_page)
    skip = (page - 1) * page_size

    sort_key = (sort or "title").lower()
    sort_dir = (order or "asc").lower()
    if sort_dir not in ("asc", "desc"):
        sort_dir = "asc"
    sort_fields = {
        "title": "title",
        "asin": "_id",
        "book_count": "book_count_calc",
        "user_count": "user_count",
        "fetched_at": "fetched_at",
        "next_refresh_at": "next_refresh_at",
    }
    if sort_key not in sort_fields:
        sort_key = "title"
    pipeline = []
    if query:
        pipeline.append({"$match": query})
    pipeline.append({
        "$addFields": {
            "book_count_calc": {
                "$size": {
                    "$filter": {
                        "input": {"$ifNull": ["$books", []]},
                        "as": "book",
                        "cond": {"$ne": ["$$book.hidden", True]},
                    }
                }
            }
        }
    })
    pipeline.append({"$sort": {sort_fields[sort_key]: 1 if sort_dir == "asc" else -1}})
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": page_size})
    docs = list(col.aggregate(pipeline))
    series = []
    for doc in docs:
        entry = {k: v for k, v in doc.items() if k != "_id"}
        entry["asin"] = doc.get("_id")
        entry["book_count"] = doc.get("book_count_calc") if doc.get("book_count_calc") is not None else visible_book_count(doc.get("books"))
        entry["user_count"] = doc.get("user_count", 0)
        entry["books"] = doc.get("books", []) if isinstance(doc.get("books", []), list) else []
        entry["ignore_narrator_warnings"] = bool(doc.get("ignore_narrator_warnings", False))
        entry["narrator_warnings"] = doc.get("narrator_warnings", []) if isinstance(doc.get("narrator_warnings", []), list) else []
        entry.pop("book_count_calc", None)
        series.append(entry)
    return {
        "series": series,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max_page,
    }


@api_router.get("/series/known")
async def api_list_known_series(user=Depends(get_current_user)):
    """Get all known series for any authenticated user"""
    col = get_series_collection()
    lib_col = get_user_library_collection()
    # Get series user already has
    user_series = set()
    for doc in lib_col.find({"username": user["username"], "series_asin": {"$exists": True}}):
        if doc.get("series_asin"):
            user_series.add(doc["series_asin"])
    
    series = []
    for doc in col.find({}).sort("title"):
        asin = doc.get("_id")
        cover = None
        books = doc.get("books", []) if isinstance(doc.get("books"), list) else []
        visible = visible_books(books)
        for b in visible:
            if isinstance(b, dict) and b.get("image"):
                cover = b.get("image")
                break
        if not cover:
            for b in books:
                if isinstance(b, dict) and b.get("image"):
                    cover = b.get("image")
                    break
        series.append({
            "asin": asin,
            "title": doc.get("title"),
            "url": doc.get("url"),
            "book_count": len(visible),
            "user_count": doc.get("user_count", 0),
            "in_library": asin in user_series,
            "cover": cover,
        })
    return series


@api_router.post("/series/{asin}/refresh")
async def api_refresh_series(asin: str, payload: SeriesRefreshRequest | None = None, user=Depends(get_current_user)):
    _require_admin(user)
    settings = load_settings()
    response_groups = (payload.response_groups if payload else None) or settings.response_groups or DEFAULT_RESPONSE_GROUPS
    job_id = enqueue_fetch_series_books(user["username"], asin, response_groups=response_groups)
    return {"asin": asin, "job_id": job_id}


@api_router.post("/series/reschedule-all")
async def api_reschedule_all_series(user=Depends(get_current_user)):
    _require_admin(user)
    from .tasks import reschedule_all_series
    result = reschedule_all_series()
    # Record as a job entry for auditing
    now_iso = _utcnow_iso()
    job_id = str(get_jobs_collection().insert_one({
        "type": "reschedule_all_series",
        "username": user.get("username"),
        "status": "done",
        "result": result,
        "created_at": now_iso,
        "finished_at": now_iso,
    }).inserted_id)
    return {**result, "job_id": job_id}


@api_router.post("/series/refresh-all")
async def api_refresh_all_series(user=Depends(get_current_user)):
    _require_admin(user)
    from .tasks import refresh_all_series, enqueue_reschedule_all_series
    refresh_result = refresh_all_series(source="manual")
    # Enqueue a follow-up reschedule job to run after a short delay (so refresh probes can complete)
    reschedule_job_id = enqueue_reschedule_all_series(user.get("username"), delay_seconds=60)
    now_iso = _utcnow_iso()
    job_id = str(get_jobs_collection().insert_one({
        "type": "refresh_all_series",
        "username": user.get("username"),
        "status": "done",
        "result": {"refresh": refresh_result, "reschedule_job_id": reschedule_job_id},
        "created_at": now_iso,
        "finished_at": now_iso,
    }).inserted_id)
    return {"refresh": refresh_result, "reschedule_job_id": reschedule_job_id, "job_id": job_id}


@api_router.get("/database/stats")
async def api_database_stats(user=Depends(get_current_user)):
    _require_admin(user)
    from .db import get_db
    
    db = get_db()
    collections = ["series", "user_library", "users", "jobs", "settings", "api_keys"]
    stats = []
    
    for col_name in collections:
        try:
            col_stats = db.command("collStats", col_name)
            stats.append({
                "name": col_name,
                "count": col_stats.get("count", 0),
                "size": col_stats.get("size", 0),
                "indexSize": col_stats.get("totalIndexSize", 0)
            })
        except Exception:
            # Collection might not exist yet
            stats.append({
                "name": col_name,
                "count": 0,
                "size": 0,
                "indexSize": 0
            })
    
    return {"collections": stats}


@api_router.post("/database/dump-restore")
async def api_database_dump_restore(user=Depends(get_current_user)):
    _require_admin(user)
    from .db import get_db
    
    db = get_db()
    
    # Get size before
    total_before = 0
    for col_name in ["series", "user_library", "users", "jobs", "settings", "api_keys"]:
        try:
            stats = db.command("collStats", col_name)
            total_before += stats.get("size", 0) + stats.get("totalIndexSize", 0)
        except Exception:
            pass
    
    try:
        # Dump all data from all collections into memory
        backup = {}
        for col_name in db.list_collection_names():
            if col_name.startswith("system."):
                continue
            collection = db[col_name]
            # Get all documents and indexes
            backup[col_name] = {
                "documents": list(collection.find({})),
                "indexes": list(collection.list_indexes())
            }
        
        # Drop all collections (this forces MongoDB to release space)
        for col_name in db.list_collection_names():
            if not col_name.startswith("system."):
                db.drop_collection(col_name)
        
        # Restore all data
        for col_name, data in backup.items():
            collection = db[col_name]
            # Insert documents if any exist
            if data["documents"]:
                collection.insert_many(data["documents"])
            # Recreate indexes (skip the default _id index)
            for index_info in data["indexes"]:
                if index_info["name"] != "_id_":
                    keys = list(index_info["key"].items())
                    options = {k: v for k, v in index_info.items() if k not in ["key", "v", "ns"]}
                    try:
                        collection.create_index(keys, **options)
                    except Exception:
                        pass  # Index might already exist or be invalid
        
        # Get size after
        total_after = 0
        for col_name in ["series", "user_library", "users", "jobs", "settings", "api_keys"]:
            try:
                stats = db.command("collStats", col_name)
                total_after += stats.get("size", 0) + stats.get("totalIndexSize", 0)
            except Exception:
                pass
        
        return {
            "status": "ok",
            "size_before": total_before,
            "size_after": total_after
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Operation failed: {str(e)}")


@api_router.post("/series/purge-and-compact")
async def api_purge_and_compact(user=Depends(get_current_user)):
    _require_admin(user)
    from .db import get_db
    
    series_col = get_series_collection()
    db = get_db()
    
    # Get size before
    stats_before = db.command("collStats", "series")
    size_before = stats_before.get("size", 0) + stats_before.get("totalIndexSize", 0)
    
    # Purge cache data
    result = series_col.update_many(
        {},
        {
            "$unset": {
                "books": "",
                "raw": "",
                "fetched_at": "",
                "cover_image": "",
                "created_at": "",
                "updated_at": ""
            }
        }
    )
    
    # Compact the collection to reclaim disk space
    db.command("compact", "series")
    
    # Get size after
    stats_after = db.command("collStats", "series")
    size_after = stats_after.get("size", 0) + stats_after.get("totalIndexSize", 0)
    
    return {
        "status": "ok",
        "purged_count": result.modified_count,
        "size_before": size_before,
        "size_after": size_after
    }


@api_router.delete("/series/{asin}")
async def api_delete_series(asin: str, user=Depends(get_current_user)):
    _require_admin(user)
    job_id = enqueue_delete_series(user.get("username", "admin"), asin)
    return {"status": "queued", "job_id": job_id}


# --- Jobs (admin) ---


@api_router.get("/jobs")
async def api_list_jobs(user=Depends(get_current_user)):
    _require_admin(user)
    settings = load_settings()
    jobs = []
    series_col = get_series_collection()
    for doc in get_jobs_collection().find({}).sort([("_id", -1)]).limit(settings.max_job_history):
        title = doc.get("title")
        if not title and doc.get("asin"):
            series_doc = series_col.find_one({"_id": doc.get("asin")})
            title = series_doc.get("title") if series_doc else f"Series {doc.get('asin')}"
        jobs.append({
            "id": str(doc.get("_id")),
            "type": doc.get("type"),
            "status": doc.get("status"),
            "username": doc.get("username"),
            "asin": doc.get("asin"),
            "title": title,
            "result": doc.get("result"),
            "created_at": doc.get("created_at"),
            "started_at": doc.get("started_at"),
            "finished_at": doc.get("finished_at"),
        })
    return jobs


@api_router.post("/jobs/clear")
async def api_clear_jobs(user=Depends(get_current_user)):
    _require_admin(user)
    result = get_jobs_collection().delete_many({})
    return {"status": "ok", "deleted": result.deleted_count}


@api_router.post("/jobs/prune")
async def api_prune_jobs(user=Depends(get_current_user)):
    _require_admin(user)
    settings = load_settings()
    # Get IDs to keep (most recent max_job_history jobs)
    jobs_to_keep = []
    for doc in get_jobs_collection().find({}).sort([("_id", -1)]).limit(settings.max_job_history):
        jobs_to_keep.append(doc["_id"])
    
    # Delete all jobs not in the keep list
    if jobs_to_keep:
        result = get_jobs_collection().delete_many({"_id": {"$nin": jobs_to_keep}})
        return {"status": "ok", "deleted": result.deleted_count}
    else:
        return {"status": "ok", "deleted": 0}


@api_router.post("/jobs/test")
async def api_test_job(user=Depends(get_current_user)):
    _require_admin(user)
    job_id = enqueue_test_job()
    return {"job_id": job_id}


# --- Profile (current user) ---


@api_router.get("/profile")
async def api_profile(user=Depends(get_current_user)):
    col = get_user_library_collection()
    lib_count = col.count_documents({"username": user["username"]})
    return {
        "username": user["username"],
        "role": user.get("role", "user"),
        "library_count": lib_count,
        "date_format": user.get("date_format", "iso"),
        "frontpage_slug": user.get("frontpage_slug") or user.get("username"),
        "show_narrator_warnings": user.get("show_narrator_warnings", True),
        "hide_narrator_warnings_for_dramatized_adaptations": user.get("hide_narrator_warnings_for_dramatized_adaptations", False),
        "latest_count": int(user.get("latest_count") or 4),
    }


@api_router.post("/profile/password")
async def api_change_password(payload: PasswordChangeRequest, user=Depends(get_current_user)):
    col = get_users_collection()
    doc = col.find_one({"username": user["username"]})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    from .security import verify_password, get_password_hash

    if not verify_password(payload.current_password, doc.get("password_hash", "")):
        raise HTTPException(status_code=400, detail="Current password incorrect")
    col.update_one({"username": user["username"]}, {"$set": {"password_hash": get_password_hash(payload.new_password)}})
    return {"status": "ok"}


@api_router.post("/profile/preferences")
async def api_update_profile_settings(payload: ProfileUpdateRequest, user=Depends(get_current_user)):
    if payload.date_format not in ("iso", "de", "us"):
        raise HTTPException(status_code=400, detail="Invalid date format")
    # Validate latest_count
    try:
        latest_count = int(payload.latest_count)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid latest_count")
    if latest_count < 1 or latest_count > 24:
        raise HTTPException(status_code=400, detail="latest_count must be between 1 and 24")
    col = get_users_collection()
    col.update_one({"_id": user["_id"]}, {"$set": {"date_format": payload.date_format, "show_narrator_warnings": payload.show_narrator_warnings, "hide_narrator_warnings_for_dramatized_adaptations": payload.hide_narrator_warnings_for_dramatized_adaptations, "latest_count": latest_count}})
    return {"status": "ok", "date_format": payload.date_format, "show_narrator_warnings": payload.show_narrator_warnings, "hide_narrator_warnings_for_dramatized_adaptations": payload.hide_narrator_warnings_for_dramatized_adaptations, "latest_count": latest_count}


@api_router.post("/profile/frontpage")
async def api_update_frontpage(payload: FrontpageSlugRequest, user=Depends(get_current_user)):
    settings = load_settings()
    if not settings.users_can_edit_frontpage_slug:
        raise HTTPException(status_code=403, detail="Frontpage slug changes are disabled")
    slug = (payload.slug or "").strip()
    if not slug:
        raise HTTPException(status_code=400, detail="Slug cannot be empty")
    # Only allow letters, numbers, hyphen, underscore
    if not re.fullmatch(r"[A-Za-z0-9_-]+", slug):
        raise HTTPException(status_code=400, detail="Slug may contain letters, numbers, hyphen, underscore")
    col = get_users_collection()
    # Ensure uniqueness per user (own slug) or globally? Here per slug unique globally so each frontpage is unique
    existing = col.find_one({"frontpage_slug": slug, "username": {"$ne": user["username"]}})
    if existing:
        raise HTTPException(status_code=400, detail="Slug already in use")
    col.update_one({"username": user["username"]}, {"$set": {"frontpage_slug": slug}})
    return {"status": "ok", "frontpage_slug": slug}



@api_router.get("/profile/api-keys")
async def api_list_api_keys(user=Depends(get_current_user)):
    from .db import get_api_keys_collection
    keys = []
    for doc in get_api_keys_collection().find({"username": user["username"]}):
        keys.append({
            "id": str(doc.get("_id")),
            "description": doc.get("description"),
            "key_preview": doc.get("key", "")[:8] + "...",
            "created_at": doc.get("created_at"),
            "last_used_at": doc.get("last_used_at"),
        })
    return keys


@api_router.post("/profile/api-keys")
async def api_create_api_key(payload: ApiKeyCreateRequest, user=Depends(get_current_user)):
    from .db import get_api_keys_collection
    import secrets
    import string
    
    # Generate 40 character alphanumeric key
    alphabet = string.ascii_letters + string.digits
    key_part = ''.join(secrets.choice(alphabet) for _ in range(40))
    api_key = f"abat_{key_part}"
    doc = {
        "username": user["username"],
        "key": api_key,
        "description": payload.description,
        "created_at": _utcnow_iso(),
        "last_used_at": None,
    }
    get_api_keys_collection().insert_one(doc)
    return {"status": "ok", "api_key": api_key, "description": payload.description}


@api_router.delete("/profile/api-keys/{key_id}")
async def api_delete_api_key(key_id: str, user=Depends(get_current_user)):
    from .db import get_api_keys_collection
    from bson import ObjectId
    try:
        oid = ObjectId(key_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid key ID")
    result = get_api_keys_collection().delete_one({"_id": oid, "username": user["username"]})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"status": "ok"}


# --- Notifications ---
class NotificationSettings(BaseModel):
    enabled: bool
    urls: List[str]
    notify_new_audiobook: bool = False
    notify_release: bool = False


@api_router.get("/notifications/settings")
async def api_get_notifications(user=Depends(get_current_user)):
    col = get_users_collection()
    doc = col.find_one({"username": user["username"]}) or {}
    notif = doc.get("notifications", {})
    return {
        "enabled": bool(notif.get("enabled", False)),
        "urls": notif.get("urls", []),
        "notify_new_audiobook": bool(notif.get("notify_new_audiobook", False)),
        "notify_release": bool(notif.get("notify_release", False)),
    }


@api_router.post("/notifications/settings")
async def api_save_notifications(payload: NotificationSettings, user=Depends(get_current_user)):
    col = get_users_collection()
    update = {
        "notifications.enabled": bool(payload.enabled),
        "notifications.urls": [u.strip() for u in payload.urls if u.strip()],
        "notifications.notify_new_audiobook": bool(payload.notify_new_audiobook),
        "notifications.notify_release": bool(payload.notify_release),
    }
    col.update_one({"username": user["username"]}, {"$set": update}, upsert=True)
    return payload


@api_router.post("/notifications/test")
async def api_test_notifications(user=Depends(get_current_user)):
    """Send a test notification (per user)"""
    col = get_users_collection()
    doc = col.find_one({"username": user["username"]}) or {}
    notif_cfg = doc.get("notifications", {})
    enabled = bool(notif_cfg.get("enabled", False))
    urls = notif_cfg.get("urls", [])
    if not enabled or not urls:
        return {"sent": False}
    try:
        import apprise
        ap = apprise.Apprise()
        for url in urls:
            ap.add(url)
        result = ap.notify(title="Test Notification", body="This is a test notification from Audiobook Tracker.")
        return {"sent": bool(result)}
    except Exception as e:
        print(f"Notification error: {e}")
        return {"sent": False}