from datetime import datetime as _dt, timezone
from typing import Optional, Dict, Any

from fastapi import Request
from fastapi.templating import Jinja2Templates

from . import settings as settings_mod
from .__version__ import __version__

def _parse_iso_datetime(value: str | None) -> Optional[_dt]:
    if not value or not isinstance(value, str):
        return None
    try:
        text = value
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return _dt.fromisoformat(text)
    except Exception:
        return None


def _get_publication_dt(book: Any, series_asin: Optional[str] = None, series_cache: Optional[Dict[str, Any]] = None) -> Optional[_dt]:
    def _val(key: str):
        if isinstance(book, dict):
            return book.get(key)
        return getattr(book, key, None) if hasattr(book, key) else None

    raw_pub = _val("publication_datetime")
    if not raw_pub and isinstance(book, dict):
        raw_obj = book.get("raw")
        if isinstance(raw_obj, dict):
            raw_pub = raw_obj.get("publication_datetime")
    pub_dt = _parse_iso_datetime(raw_pub) if raw_pub else None
    if pub_dt:
        try:
            if pub_dt.tzinfo:
                pub_dt = pub_dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            pass
        return pub_dt

    if series_asin:
        try:
            cache = series_cache or {}
            series_doc = cache.get(series_asin)
            if series_doc is None:
                from .db import get_series_collection
                series_doc = get_series_collection().find_one({"_id": series_asin})
                cache[series_asin] = series_doc
            if isinstance(series_doc, dict):
                books = series_doc.get("books") or []
                book_asin = _val("asin") or (book.get("raw", {}).get("asin") if isinstance(book, dict) else None)
                for sb in books:
                    if not isinstance(sb, dict):
                        continue
                    if book_asin and sb.get("asin") == book_asin:
                        sb_pub = sb.get("publication_datetime") or (sb.get("raw") or {}).get("publication_datetime")
                        if sb_pub:
                            sb_dt = _parse_iso_datetime(sb_pub)
                            if sb_dt:
                                if sb_dt.tzinfo:
                                    try:
                                        sb_dt = sb_dt.astimezone(timezone.utc).replace(tzinfo=None)
                                    except Exception:
                                        pass
                                return sb_dt
                s_pub = series_doc.get("publication_datetime") or (series_doc.get("raw") or {}).get("publication_datetime")
                if s_pub:
                    s_dt = _parse_iso_datetime(s_pub)
                    if s_dt:
                        if s_dt.tzinfo:
                            try:
                                s_dt = s_dt.astimezone(timezone.utc).replace(tzinfo=None)
                            except Exception:
                                pass
                        return s_dt
        except Exception:
            pass

    try:
        rd = _val("release_date")
        if rd and isinstance(rd, str):
            ds = rd[:10]
            y, m, d = ds.split("-")
            return _dt(int(y), int(m), int(d))
    except Exception:
        return None
    return None


def render_frontpage_for_slug(request: Request, slug: str, templates: Jinja2Templates):
    if not slug:
        return None
    # Local imports to allow tests to patch these functions on the module
    from .db import get_users_collection
    from .library import get_user_library, visible_books
    from .app_helpers import (
        parse_date,
        format_dt,
        format_d,
        format_runtime,
        preload_series_data,
        compute_num_latest,
    )
    settings = settings_mod.load_settings()
    users_col = get_users_collection()
    user_doc = users_col.find_one({"$or": [{"frontpage_slug": slug}, {"username": slug}]})
    if not user_doc:
        return None
    username = user_doc.get("username")
    date_format = user_doc.get("date_format", "de")
    library = get_user_library(username)
    num_latest = compute_num_latest(user_doc)

    now = _dt.now(timezone.utc).replace(tzinfo=None)
    upcoming_cards = []
    latest_cards = []
    series_rows = []
    total_books = 0
    last_refresh_dt = None

    series_asins = [getattr(series_item, 'asin', None) for series_item in library if getattr(series_item, 'asin', None)]
    series_cache, narrator_warnings_map = preload_series_data(series_asins)

    def _get_publication_dt_local(book, series_item_ref=None):
        series_asin = getattr(series_item_ref, 'asin', None) if series_item_ref is not None else None
        return _get_publication_dt(book, series_asin=series_asin or getattr(book, 'asin', None), series_cache=series_cache)

    for series_item in library:
        books = series_item.books if isinstance(series_item.books, list) else []
        visible = visible_books(books)
        total_books += len(visible)
        if series_item.fetched_at:
            dt = parse_date(series_item.fetched_at)
            if dt and (not last_refresh_dt or dt > last_refresh_dt):
                last_refresh_dt = dt
        series_last_release = None
        series_next_release = None
        for book in visible:
            rd = _get_publication_dt_local(book, series_item_ref=series_item)
            if not rd:
                continue
            if rd <= now and (not series_last_release or rd > series_last_release):
                series_last_release = rd
            if rd > now and (not series_next_release or rd < series_next_release):
                series_next_release = rd
            book_url = getattr(book, "url", None)
            if not book_url and getattr(book, "asin", None):
                book_url = f"https://www.audible.com/pd/{getattr(book, 'asin', '')}"
            if rd > now:
                time_left_str, hours_left, days_left = None, None, None
                try:
                    from .app_helpers import format_time_left as _fmt
                    time_left_str, hours_left, days_left = _fmt(rd, now)
                except Exception:
                    pass
                runtime_str = format_runtime(getattr(book, "runtime", None))
                upcoming_cards.append({
                    "title": getattr(book, "title", None) or series_item.title,
                    "series": series_item.title,
                    "narrators": getattr(book, "narrators", None) or "",
                    "runtime": getattr(book, "runtime", None) or "",
                    "runtime_str": runtime_str,
                    "release_dt": rd,
                    "release_dt_iso": rd.isoformat() + 'Z',
                    "release_str": format_d(rd, date_format),
                    "time_left_str": time_left_str,
                    "hours_left": hours_left,
                    "days_left": days_left or 0,
                    "image": getattr(book, "image", None),
                    "url": book_url,
                })
            else:
                days_ago = (now - rd).days
                runtime_str = format_runtime(getattr(book, "runtime", None))
                latest_cards.append({
                    "title": getattr(book, "title", None) or series_item.title,
                    "series": series_item.title,
                    "narrators": getattr(book, "narrators", None) or "",
                    "runtime": getattr(book, "runtime", None) or "",
                    "runtime_str": runtime_str,
                    "release_dt_iso": rd.isoformat() + 'Z',
                    "release_dt": rd,
                    "release_str": format_d(rd, date_format),
                    "days_ago": days_ago,
                    "image": getattr(book, "image", None),
                    "url": book_url,
                })
        narr_set = set()
        runtime_mins = 0
        for book in visible:
            if getattr(book, "narrators", None):
                for n in str(getattr(book, "narrators", "")).split(","):
                    n = n.strip()
                    if n:
                        narr_set.add(n)
            try:
                runtime_mins += int(getattr(book, "runtime", None) or 0)
            except Exception:
                pass
        hours = runtime_mins // 60
        mins = runtime_mins % 60
        runtime_str = f"{hours}h {mins}m" if hours else f"{mins}m"
        cover = None
        for book in visible:
            if getattr(book, "image", None):
                cover = getattr(book, "image", None)
                break
        if not cover:
            for book in books:
                if getattr(book, "image", None):
                    cover = getattr(book, "image", None)
                    break
        last_release_str = format_d(series_last_release, date_format)
        last_release_ts = series_last_release.isoformat() if series_last_release else None
        next_release_str = format_d(series_next_release, date_format)
        next_release_ts = series_next_release.isoformat() if series_next_release else None
        series_rows.append({
            "title": series_item.title,
            "asin": series_item.asin,
            "narrators": ", ".join(sorted(narr_set)),
            "book_count": len(visible),
            "runtime": runtime_str,
            "cover": cover,
            "last_release": last_release_str,
            "last_release_ts": last_release_ts,
            "next_release": next_release_str,
            "next_release_ts": next_release_ts,
            "duration_minutes": runtime_mins,
            "url": series_item.url,
        })

    upcoming_cards.sort(key=lambda x: x["release_dt"])
    latest_cards.sort(key=lambda x: x["release_dt"], reverse=True)
    latest_cards = latest_cards[:num_latest]
    series_rows.sort(key=lambda x: (x["title"] or ""))

    for row in series_rows:
        row["narrator_warnings"] = narrator_warnings_map.get(row.get("asin")) or []

    title_to_asin = {row.get("title"): row.get("asin") for row in series_rows if row.get("title")}

    import re
    def _card_contains_dramatized(card):
        for k in ("title", "series", "narrators"):
            v = card.get(k)
            if isinstance(v, str) and re.search(r"dramatized adaptation", v, re.IGNORECASE):
                return True
        return False
    dramatized_titles = set()
    for card in upcoming_cards + latest_cards:
        if _card_contains_dramatized(card):
            dramatized_titles.add(card.get("title"))

    hide_pref = bool(user_doc.get('hide_narrator_warnings_for_dramatized_adaptations', False))

    for card in upcoming_cards:
        series_asin = card.get("series_asin") or title_to_asin.get(card.get("series"))
        card["series_asin"] = series_asin
        base_flag = bool(series_asin and card.get("title") in (narrator_warnings_map.get(series_asin) or []))
        card["narrator_warning"] = base_flag and not (hide_pref and card.get("title") in dramatized_titles)
    for card in latest_cards:
        series_asin = card.get("series_asin") or title_to_asin.get(card.get("series"))
        card["series_asin"] = series_asin
        base_flag = bool(series_asin and card.get("title") in (narrator_warnings_map.get(series_asin) or []))
        card["narrator_warning"] = base_flag and not (hide_pref and card.get("title") in dramatized_titles)

    if hide_pref and dramatized_titles:
        for row in series_rows:
            row["narrator_warnings"] = [t for t in (row.get("narrator_warnings") or []) if t not in dramatized_titles]

    stats = {
        "series_count": len(library),
        "books_count": total_books,
        "last_refresh": format_dt(last_refresh_dt, date_format),
        "slug": user_doc.get("frontpage_slug") or username,
        "username": username,
    }

    settings = settings_mod.load_settings()

    return templates.TemplateResponse(
        "frontpage.html",
        {
            "request": request,
            "settings": settings,
            "base_path": "",
            "public_nav": True,
            "brand_title": "Audiobook Tracker",
            "hide_nav": True,
            "page_title": "Audiobook Tracker",
            "main_class": "container-fluid px-3 px-sm-4",
            "stats": stats,
            "upcoming": upcoming_cards,
            "latest": latest_cards,
            "series": series_rows,
            "version": __version__,
            "show_narrator_warnings": user_doc.get("show_narrator_warnings", True),
        },
    )
