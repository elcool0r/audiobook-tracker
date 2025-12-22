from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime
from queue import SimpleQueue, Empty
from typing import Dict, Any

from bson import ObjectId

from .library import fetch_series_books, set_series_books, set_series_raw, touch_series_fetched, set_series_next_refresh, _fetch_series_books_internal, ensure_series_document
from lib.audible_api_search import get_product_by_asin
from .settings import load_settings
from .db import get_jobs_collection, get_series_collection, get_users_collection, get_user_library_collection
from lib.audible_api_search import DEFAULT_RESPONSE_GROUPS

AUTO_REFRESH_CYCLE_SEC = 24 * 60 * 60


Job = Dict[str, Any]


class TaskWorker:
    def __init__(self):
        self.queue: SimpleQueue[Job] = SimpleQueue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._scheduler_thread: threading.Thread | None = None
        self._scheduler_interval_sec: int = 60

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        # Start scheduler for auto refresh if enabled
        settings = load_settings()
        if settings.auto_refresh_enabled:
            if not self._scheduler_thread or not self._scheduler_thread.is_alive():
                self._scheduler_thread = threading.Thread(target=self._scheduler_run, daemon=True)
                self._scheduler_thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=2)

    def ensure_scheduler_running(self, rebalance: bool = False):
        """Start the auto-refresh scheduler thread if enabled and not already running.

        Optionally rebalance all series refresh slots before starting so toggling the setting on
        immediately redistributes next_refresh_at times instead of waiting for the next cycle.
        """
        try:
            settings = load_settings()
            if not settings.auto_refresh_enabled:
                return
        except Exception:
            return

        if rebalance:
            try:
                _rebalance_auto_refresh()
            except Exception:
                pass

        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return

        # Ensure the stop flag is clear for a fresh scheduler run
        if self._stop.is_set():
            self._stop.clear()

        self._scheduler_thread = threading.Thread(target=self._scheduler_run, daemon=True)
        self._scheduler_thread.start()

    def enqueue(self, job: Job):
        self.queue.put(job)

    def _run(self):
        while not self._stop.is_set():
            try:
                job = self.queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self._handle(job)
            except Exception as exc:
                job_id = job.get("job_id") if isinstance(job, dict) else None
                if job_id:
                    try:
                        self._finish_job(job_id, {"error": str(exc)})
                    except Exception:
                        pass
                logging.exception("TaskWorker failed for job %s", job.get("type") if isinstance(job, dict) else "unknown")

    def _handle(self, job: Job):
        job_id = job.get("job_id")
        if job_id:
            col = get_jobs_collection()
            col.update_one({"_id": ObjectId(job_id)}, {"$set": {"status": "running", "started_at": _now_iso()}})
        if job.get("type") == "fetch_series_books":
            self._do_fetch_series_books(job)
        elif job.get("type") == "refresh_series_probe":
            self._do_refresh_series_probe(job)
        elif job.get("type") == "delete_series":
            self._do_delete_series(job)
        elif job.get("type") == "test_job":
            self._do_test_job(job)

    def _do_fetch_series_books(self, job: Job):
        job_id = job.get("job_id")
        username = job.get("username")
        asin = job.get("asin")
        if not username or not asin:
            if job_id:
                self._finish_job(job_id, {"error": "Missing username or asin"})
            return
        settings = None
        try:
            settings = load_settings()
            if settings.debug_logging:
                logging.info(f"Starting job {job_id} for series {asin}")
            response_groups = job.get("response_groups") or settings.response_groups or DEFAULT_RESPONSE_GROUPS
            books, parent_obj, parent_asin = _fetch_series_books_internal(asin, response_groups, None)
            set_series_books(asin, books)
            touch_series_fetched(asin)
            
            # Extract title and URL from parent object and update series document
            if isinstance(parent_obj, dict):
                series_title = parent_obj.get("title") or parent_obj.get("publication_name") or parent_obj.get("product_title")
                series_url = parent_obj.get("url")
                if series_title or series_url:
                    ensure_series_document(asin, series_title, series_url)
            
            # Save raw parent series JSON if we fetched it
            if isinstance(parent_obj, dict):
                # Store raw under both the requested asin and the parent asin (if different)
                set_series_raw(asin, parent_obj)
                if parent_asin and parent_asin != asin:
                    set_series_raw(parent_asin, parent_obj)
            
            # Fallback: if we have no raw data and found children, try fetching parent ASIN directly
            # This handles cases where _fetch_series_books_internal succeeded in finding books but didn't return parent_obj
            if not isinstance(parent_obj, dict) and parent_asin:
                try:
                    proxies = None
                    try:
                        from .library import _build_proxies
                        proxies = _build_proxies(settings)
                    except Exception:
                        pass
                    resp = asyncio.run(get_product_by_asin(parent_asin, response_groups=response_groups, auth_token=None, marketplace=None, proxies=proxies, user_agent=settings.user_agent))
                    product = resp.get("product") if isinstance(resp, dict) and "product" in resp else resp
                    if isinstance(product, dict):
                        set_series_raw(parent_asin, product)
                        # Also store under requested asin if different
                        if parent_asin != asin:
                            set_series_raw(asin, product)
                except Exception:
                    pass
            
            # Schedule next refresh: find this series' slot in the next 24-hour cycle
            # This maintains consistent distribution across all series
            try:
                next_when = _now_dt() + _delta_sec(AUTO_REFRESH_CYCLE_SEC)
                set_series_next_refresh(str(asin), next_when.isoformat() + "Z")
            except Exception:
                pass
            
            # Fetch and store raw parent series JSON
            # (Already done in _fetch_series_books_internal)

            try:
                _rebalance_auto_refresh()
            except Exception:
                pass
            if job_id:
                if settings.debug_logging:
                    logging.info(f"Completed job {job_id} for series {asin} with {len(books)} books")
                self._finish_job(job_id, {"book_count": len(books)})
        except Exception as e:
            if settings and getattr(settings, "debug_logging", False):
                logging.error(f"Job {job_id} failed for series {asin}: {e}")
            if job_id:
                self._finish_job(job_id, {"error": str(e)})

    def _do_test_job(self, job: Job):
        job_id = job.get("job_id")
        import time
        time.sleep(1)
        if job_id:
            self._finish_job(job_id, {"message": "ok"})

    def _do_delete_series(self, job: Job):
        job_id = job.get("job_id")
        username = job.get("username")
        asin = job.get("asin")
        if not asin:
            return
        series_col = get_series_collection()
        lib_col = get_user_library_collection()
        result_series = series_col.delete_one({"_id": asin})
        result_lib = lib_col.delete_many({"series_asin": asin})
        # clean next_refresh if any
        if job_id:
            self._finish_job(job_id, {
                "series_deleted": result_series.deleted_count,
                "library_entries_removed": result_lib.deleted_count,
                "asin": asin,
                "requested_by": username,
            })

    def _do_refresh_series_probe(self, job: Job):
        """Probe a series product; if relationships changed, refresh books. Always touch fetched_at."""
        job_id = job.get("job_id")
        asin = job.get("asin")
        if not asin:
            if job_id:
                self._finish_job(job_id, {"error": "Missing asin"})
            return
        settings = None
        try:
            settings = load_settings()
            response_groups = job.get("response_groups") or settings.response_groups or DEFAULT_RESPONSE_GROUPS
            try:
                from .library import _build_proxies
                proxies = _build_proxies(settings)
                # Load current stored relationships
                series_col = get_series_collection()
                doc = series_col.find_one({"_id": asin}) or {}
                old_raw = doc.get("raw") if isinstance(doc.get("raw"), dict) else None
                old_rels = old_raw.get("relationships") if isinstance(old_raw, dict) else None
                old_books = doc.get("books") if isinstance(doc.get("books"), list) else []

                # Lightweight diff: fetch only parent product first; fetch children/books only if relationships changed or no books yet
                async def _load_product(target_asin: str):
                    try:
                        return await get_product_by_asin(target_asin, response_groups=response_groups, auth_token=None, proxies=_build_proxies(settings), user_agent=settings.user_agent)
                    except Exception:
                        return None

                parent_obj = asyncio.run(_load_product(asin))
                parent_obj = parent_obj.get("product") if isinstance(parent_obj, dict) and "product" in parent_obj else parent_obj

                parent_asin = None
                if isinstance(parent_obj, dict):
                    for rel in parent_obj.get("relationships", []) or []:
                        if isinstance(rel, dict) and rel.get("relationship_type") == "series" and rel.get("relationship_to_product") == "parent" and rel.get("asin"):
                            parent_asin = rel.get("asin")
                            break
                    if not parent_asin and (parent_obj.get("content_delivery_type") == "BookSeries" or any(isinstance(r, dict) and r.get("relationship_to_product") == "child" for r in parent_obj.get("relationships", []))):
                        parent_asin = asin

                parent_id = parent_asin or asin
                new_rels = parent_obj.get("relationships") if isinstance(parent_obj, dict) else None
                changed = not _relationships_equal(old_rels, new_rels) if new_rels is not None else False

                books_current = old_books

                if changed or not old_books:
                    # Full fetch only when relationships changed or we have no books yet
                    books, parent_obj_full, parent_asin_full = _fetch_series_books_internal(asin, response_groups, None)
                    parent_id = parent_asin_full or parent_asin or asin
                    if isinstance(parent_obj_full, dict):
                        set_series_raw(asin, parent_obj_full)
                        if parent_asin_full and parent_asin_full != asin:
                            set_series_raw(parent_asin_full, parent_obj_full)
                    books_current = books
                    set_series_books(asin, books)
                else:
                    # Update raw parent only if we fetched it, but skip expensive child fetch
                    if isinstance(parent_obj, dict):
                        set_series_raw(asin, parent_obj)
                        if parent_asin and parent_asin != asin:
                            set_series_raw(parent_asin, parent_obj)

                # Touch fetched timestamp
                touch_series_fetched(asin)
                # Send notifications for new books and releases
                # Determine whether there are newly discovered audiobooks in this probe
                try:
                    old_asins = { (b.get('asin') if isinstance(b, dict) else getattr(b, 'asin', None)) for b in (old_books or []) }
                    cur_asins = { (b.get('asin') if isinstance(b, dict) else getattr(b, 'asin', None)) for b in (books_current or []) }
                    # Remove falsy values
                    old_asins = {a for a in old_asins if a}
                    cur_asins = {a for a in cur_asins if a}
                    new_asins = list(cur_asins - old_asins)
                except Exception:
                    new_asins = []

                # Send notifications when we fetched full data (or had no books before)
                if changed or not old_books:
                    try:
                        self._send_series_notifications(parent_id, doc, old_books, books_current)
                    except Exception:
                        pass

                # A probe is considered to have 'changed' if new audiobooks were discovered or this is the first time we have books
                final_changed = bool(new_asins) or (not old_books and bool(books_current))

                # Schedule next refresh at least one full cycle in the future for both the probed ASIN and its parent
                try:
                    next_when = _now_dt() + _delta_sec(AUTO_REFRESH_CYCLE_SEC)
                    set_series_next_refresh(str(asin), next_when.isoformat() + "Z")
                    if parent_id and parent_id != asin:
                        set_series_next_refresh(str(parent_id), next_when.isoformat() + "Z")
                except Exception:
                    pass
                if job_id:
                    self._finish_job(job_id, {"book_count": len(books_current), "changed": bool(final_changed)})
            except Exception as inner_exc:
                if settings and getattr(settings, "debug_logging", False):
                    logging.exception(f"refresh_series_probe failed for {asin}: {inner_exc}")
                if job_id:
                    self._finish_job(job_id, {"error": str(inner_exc)})
        except Exception as e:
            if job_id:
                self._finish_job(job_id, {"error": str(e)})

    def _scheduler_run(self):
        """Distribute refresh across configured interval and enqueue due probes every minute.
        
        Strategy: All series are distributed evenly across a 24-hour cycle. When a series is refreshed,
        it gets rescheduled to its next slot in the cycle (24h later), maintaining consistent distribution.
        """
        import time
        try:
            settings = load_settings()
            if not settings.auto_refresh_enabled:
                return
        except Exception:
            return

        _rebalance_auto_refresh()

        # Loop: every minute, enqueue due probes and keep the schedule balanced
        while not self._stop.is_set():
            try:
                settings = load_settings()
                if not settings.auto_refresh_enabled:
                    break

                now = _now_dt()
                now_iso = now.isoformat() + "Z"
                series_col = get_series_collection()
                due = list(series_col.find({"next_refresh_at": {"$lte": now_iso}}, {"_id": 1}).sort("next_refresh_at", 1).limit(10))

                for d in due:
                    asin = d.get("_id")
                    if not asin:
                        continue
                    enqueue_refresh_probe(str(asin), response_groups=None, source="auto")

            except Exception:
                pass
            # sleep interval
            for _ in range(self._scheduler_interval_sec):
                if self._stop.is_set():
                    break
                time.sleep(1)

    def _finish_job(self, job_id: str, result: Dict[str, Any]):
        col = get_jobs_collection()
        col.update_one({"_id": ObjectId(job_id)}, {"$set": {"status": "done", "result": result, "finished_at": _now_iso()}}, upsert=True)

    def _send_series_notifications(self, asin: str, series_doc: Dict[str, Any], old_books: list, books_current: list):
        """Notify users tracking this series about new audiobooks and releases based on their settings.
        - New audiobook: when current books contain ASINs not in old_books.
        - Release: when a book's release_date is today/past and user hasn't been notified for that ASIN.
        """
        def _get(book, key):
            return book.get(key) if isinstance(book, dict) else getattr(book, key, None)

        # Build sets for diff (support dicts or model instances)
        old_asins = {_get(b, "asin") for b in (old_books or []) if _get(b, "asin")}
        cur_asins = {_get(b, "asin") for b in (books_current or []) if _get(b, "asin")}
        new_asins = [a for a in cur_asins - old_asins if a]
        
        # If this is the first time we have books for this series, bail out (no notifications on initial add)
        if not old_asins:
            return

        # Map asin -> book for quick lookup
        book_map = {}
        for b in (books_current or []):
            asin_val = _get(b, "asin")
            if asin_val:
                book_map[asin_val] = b

        # Release candidates: release_date <= today
        from datetime import date
        today = date.today()

        def parse_date(s):
            try:
                if not s or not isinstance(s, str):
                    return None
                # Expect YYYY-MM-DD or similar; take first 10 chars
                ds = s[:10]
                y, m, d = ds.split("-")
                return date(int(y), int(m), int(d))
            except Exception:
                return None

        release_candidates = []
        for b in (books_current or []):
            rd = parse_date(_get(b, "release_date"))
            asin_val = _get(b, "asin")
            if rd and rd == today and asin_val:
                release_candidates.append(b)

        # Find all users tracking this series
        lib_col = get_user_library_collection()
        users_col = get_users_collection()
        series_title = series_doc.get("title") or f"Series {asin}"

        for entry in lib_col.find({"series_asin": asin}):
            username = entry.get("username")
            if not username:
                continue
            user_doc = users_col.find_one({"username": username}) or {}
            notif = user_doc.get("notifications", {})
            enabled = bool(notif.get("enabled", False))
            urls = [u for u in notif.get("urls", []) if isinstance(u, str) and u.strip()]
            if not enabled or not urls:
                continue
            notify_new = bool(notif.get("notify_new_audiobook", False))
            notify_rel = bool(notif.get("notify_release", False))
            if not notify_new and not notify_rel:
                continue

            to_send_msgs = []
            # New audiobook notifications (skip ones already notified for this user)
            pending_new = new_asins
            notified_new = entry.get("notified_new_asins", []) if isinstance(entry.get("notified_new_asins"), list) else []
            if notify_new and pending_new:
                to_send = [a for a in pending_new if a not in notified_new]
                titles = [book_map[a].get("title") or a for a in to_send if a in book_map]
                if titles:
                    body = f"New audiobooks found in '{series_title}':\n- " + "\n- ".join(titles)
                    to_send_msgs.append(("New Audiobook(s)", body, to_send))

            # Release notifications on the configured release day
            if notify_rel and release_candidates:
                notified_releases = entry.get("notified_releases", []) if isinstance(entry.get("notified_releases"), list) else []
                pending = [b for b in release_candidates if b.get("asin") not in notified_releases]
                if pending:
                    titles = [f"{b.get('title') or b.get('asin')} (release date {b.get('release_date')})" for b in pending]
                    body = f"Audiobooks releasing today in '{series_title}':\n- " + "\n- ".join(titles)
                    to_send_msgs.append(("Audiobook Release", body, [b.get("asin") for b in pending if b.get("asin")]))

            if not to_send_msgs:
                continue

            # Send via Apprise
            try:
                import apprise
                ap = apprise.Apprise()
                for u in urls:
                    ap.add(u)
                # send each message
                for msg in to_send_msgs:
                    title, body = msg[0], msg[1]
                    ap.notify(title=title, body=body)
            except Exception:
                # don't block if apprise fails
                pass

            # Update per-user notification state: releases
            if notify_new and new_asins:
                # Persist new-audiobook notifications to avoid repeat alerts for the same titles/ASINs
                try:
                    to_mark_new = []
                    for msg in to_send_msgs:
                        if msg[0] == "New Audiobook(s)" and len(msg) > 2:
                            to_mark_new.extend([a for a in msg[2] if a])
                    if to_mark_new:
                        lib_col.update_one({"_id": entry.get("_id")}, {"$addToSet": {"notified_new_asins": {"$each": to_mark_new}}})
                except Exception:
                    pass

            if notify_rel and release_candidates:
                to_mark = [b.get("asin") for b in release_candidates if b.get("asin")]
                if to_mark:
                    try:
                        lib_col.update_one({"_id": entry.get("_id")}, {"$addToSet": {"notified_releases": {"$each": to_mark}}})
                    except Exception:
                        pass


worker = TaskWorker()


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

def _now_dt():
    return datetime.utcnow()

def _delta_sec(sec: int):
    from datetime import timedelta
    return timedelta(seconds=int(sec))

def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        text = value
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _rebalance_auto_refresh(reference: datetime | None = None):
    if reference is None:
        reference = _now_dt()
    series_col = get_series_collection()
    docs = list(series_col.find({}, {"_id": 1, "fetched_at": 1}))
    if not docs:
        return
    total = len(docs)
    interval = AUTO_REFRESH_CYCLE_SEC / total
    if interval <= 0:
        interval = AUTO_REFRESH_CYCLE_SEC
    now = reference
    offset_acc = interval
    from datetime import timezone
    datetime_min_utc = datetime.min.replace(tzinfo=timezone.utc)
    for doc in sorted(docs, key=lambda d: _parse_iso_datetime(d.get("fetched_at")) or datetime_min_utc):
        offset = max(int(offset_acc), 1)
        target = now + _delta_sec(offset)
        set_series_next_refresh(doc.get("_id"), target.isoformat() + "Z")
        offset_acc += interval


def _relationships_equal(a, b) -> bool:
    """Compare relationships lists ignoring order and extraneous fields by normalizing."""
    def norm_list(lst):
        if not isinstance(lst, list):
            return []
        norm = []
        for rel in lst:
            if not isinstance(rel, dict):
                continue
            norm.append({
                "asin": rel.get("asin"),
                "relationship_to_product": rel.get("relationship_to_product"),
                "relationship_type": rel.get("relationship_type"),
                "sequence": rel.get("sequence"),
                "sort": rel.get("sort"),
                "title": rel.get("title"),
            })
        # sort by asin + sequence + relationship_to_product
        def key(x):
            seq = x.get("sequence") or x.get("sort") or 0
            try:
                seq = int(seq)
            except Exception:
                seq = 0
            return (x.get("relationship_to_product"), x.get("relationship_type"), x.get("asin"), seq, x.get("title"))
        return sorted(norm, key=key)
    return norm_list(a) == norm_list(b)


def enqueue_fetch_series_books(username: str, asin: str, response_groups: str | None = None):
    # Get series title for display
    series_col = get_series_collection()
    series_doc = series_col.find_one({"_id": asin}) or {}
    title = series_doc.get("title") or f"Series {asin}"
    job_id = str(get_jobs_collection().insert_one({
        "type": "fetch_series_books",
        "username": username,
        "asin": asin,
        "title": title,
        "response_groups": response_groups,
        "status": "queued",
        "created_at": _now_iso(),
    }).inserted_id)
    worker.enqueue({"type": "fetch_series_books", "username": username, "asin": asin, "response_groups": response_groups, "job_id": job_id})
    return job_id


def enqueue_refresh_probe(asin: str, response_groups: str | None = None, source: str | None = None):
    # Get series title for display
    series_col = get_series_collection()
    series_doc = series_col.find_one({"_id": asin}) or {}
    title = series_doc.get("title") or f"Series {asin}"
    job_id = str(get_jobs_collection().insert_one({
        "type": "refresh_series_probe",
        "asin": asin,
        "title": title,
        "response_groups": response_groups,
        "source": source or "auto",
        "status": "queued",
        "created_at": _now_iso(),
    }).inserted_id)
    worker.enqueue({"type": "refresh_series_probe", "asin": asin, "response_groups": response_groups, "job_id": job_id})
    return job_id


def enqueue_delete_series(username: str, asin: str):
    # Get series title for display
    series_col = get_series_collection()
    series_doc = series_col.find_one({"_id": asin}) or {}
    title = series_doc.get("title") or f"Series {asin}"
    job_id = str(get_jobs_collection().insert_one({
        "type": "delete_series",
        "username": username,
        "asin": asin,
        "title": title,
        "status": "queued",
        "created_at": _now_iso(),
    }).inserted_id)
    worker.enqueue({"type": "delete_series", "username": username, "asin": asin, "job_id": job_id})
    return job_id


def enqueue_test_job():
    job_id = str(get_jobs_collection().insert_one({
        "type": "test_job",
        "status": "queued",
        "created_at": _now_iso(),
    }).inserted_id)
    worker.enqueue({"type": "test_job", "job_id": job_id})
    return job_id


def reschedule_all_series():
    """Reschedule all series evenly across the manual_refresh_interval_minutes from settings."""
    settings = load_settings()
    series_col = get_series_collection()
    
    # Get all series
    all_series = list(series_col.find({}))
    if not all_series:
        return {"count": 0, "message": "No series to reschedule"}
    
    # Calculate the interval in seconds
    interval_minutes = settings.manual_refresh_interval_minutes
    interval_sec = interval_minutes * 60
    
    # Distribute series evenly across the interval
    count = len(all_series)
    now = _now_dt()
    
    for i, series in enumerate(all_series):
        # Calculate offset for this series
        offset_sec = int((i / max(count, 1)) * interval_sec)
        next_refresh = now + _delta_sec(offset_sec)
        next_refresh_iso = next_refresh.isoformat() + "Z"
        
        # Update the series
        series_col.update_one(
            {"_id": series["_id"]},
            {"$set": {"next_refresh_at": next_refresh_iso}}
        )
    
    return {"count": count, "message": f"Rescheduled {count} series over {interval_minutes} minutes"}
