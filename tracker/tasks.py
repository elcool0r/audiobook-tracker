from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone
from queue import SimpleQueue, Empty
from typing import Any, Dict

from bson import ObjectId
from pymongo import UpdateOne

from .library import (
    _fetch_series_books_internal,
    ensure_series_document,
    fetch_series_books,
    is_book_hidden,
    set_series_books,
    compute_narrator_warnings,
    set_series_raw,
    set_series_next_refresh,
    touch_series_fetched,
)
from .settings import load_settings
from .db import get_jobs_collection, get_series_collection, get_users_collection, get_user_library_collection
from lib.audible_api_search import get_product_by_asin, DEFAULT_RESPONSE_GROUPS, run_coro_sync

AUTO_REFRESH_CYCLE_SEC = 24 * 60 * 60

# Track last prune date so we only prune jobs once per day
_last_jobs_prune_date = None


def _book_asin(book: Any) -> str | None:
    if isinstance(book, dict):
        return book.get("asin")
    return getattr(book, "asin", None)


def _book_raw(book: Any) -> Any:
    if isinstance(book, dict):
        return book.get("raw")
    return None


def _book_raw_publication_datetime(raw: Any) -> str | None:
    if isinstance(raw, dict):
        value = raw.get("publication_datetime")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _books_raw_changed(old_books: list | None, new_books: list | None) -> bool:
    if not old_books or not new_books:
        return False
    old_map: Dict[str, Any] = {}
    for book in old_books:
        asin = _book_asin(book)
        if asin:
            old_map[asin] = book
    for book in new_books:
        asin = _book_asin(book)
        if not asin:
            continue
        old_book = old_map.get(asin)
        if not old_book:
            continue
        old_pub = _book_raw_publication_datetime(_book_raw(old_book))
        new_pub = _book_raw_publication_datetime(_book_raw(book))
        if old_pub != new_pub:
            return True
    return False


Job = Dict[str, Any]


class TaskWorker:
    def __init__(self):
        self.queue: SimpleQueue[Job] = SimpleQueue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._scheduler_thread: threading.Thread | None = None
        self._scheduler_interval_sec: int = 60
        self._release_notifier_thread: threading.Thread | None = None
        self._release_check_interval_sec: int = 300

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
        # Start release notifier thread (independent of refresh cadence)
        if not self._release_notifier_thread or not self._release_notifier_thread.is_alive():
            self._release_notifier_thread = threading.Thread(target=self._release_notifier_run, daemon=True)
            self._release_notifier_thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=2)
        if self._release_notifier_thread:
            self._release_notifier_thread.join(timeout=2)

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
        elif job.get("type") == "reschedule_all_series":
            self._do_reschedule_all_series(job)

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
            # Check for placeholder issue_date and warn but continue
            if isinstance(parent_obj, dict) and parent_obj.get("issue_date") == "2200-01-01":
                logging.warning(f"Series {asin} has placeholder issue_date (2200-01-01), proceeding with fetch")
            target_asin = parent_asin or asin
            processed_books = set_series_books(target_asin, books)
            touch_series_fetched(target_asin)
            narrator_warnings = compute_narrator_warnings(processed_books, target_asin)
            get_series_collection().update_one({"_id": target_asin}, {"$set": {"narrator_warnings": narrator_warnings}})
            
            # Extract title and URL from parent object and update series document
            if isinstance(parent_obj, dict):
                series_title = parent_obj.get("title") or parent_obj.get("publication_name") or parent_obj.get("product_title")
                series_url = parent_obj.get("url")
                if series_title or series_url:
                    ensure_series_document(target_asin, series_title, series_url)
            
            # Save raw parent series JSON if we fetched it
            if isinstance(parent_obj, dict):
                # Store raw under the target asin (parent when available) and also under the requested asin if different
                set_series_raw(target_asin, parent_obj)
                if parent_asin and parent_asin != asin:
                    set_series_raw(asin, parent_obj)
            
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
                    try:
                        resp = run_coro_sync(get_product_by_asin(parent_asin, response_groups=response_groups, auth_token=None, marketplace=None, proxies=proxies, user_agent=settings.user_agent))
                    except Exception:
                        resp = None
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

    def _do_reschedule_all_series(self, job: Job):
        """Worker handler that waits for an optional delay and runs reschedule_all_series."""
        import time
        job_id = job.get("job_id")
        delay = int(job.get("delay_seconds", 60) or 60)
        try:
            if delay > 0:
                time.sleep(delay)
            result = reschedule_all_series()
            if job_id:
                self._finish_job(job_id, result)
        except Exception as exc:
            if job_id:
                self._finish_job(job_id, {"error": str(exc)})

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

                try:
                    parent_obj = run_coro_sync(_load_product(asin))
                except Exception:
                    parent_obj = None
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
                    target_id = parent_id
                    processed_books = set_series_books(target_id, books)
                    books_current = processed_books

                    # Compute narrator warnings using the shared helper
                    if books:
                        narrator_warnings = compute_narrator_warnings(books_current, target_id)
                        series_col.update_one({"_id": target_id}, {"$set": {"narrator_warnings": narrator_warnings}})
                else:
                    # Update raw parent only if we fetched it, but skip expensive child fetch
                    if isinstance(parent_obj, dict):
                        set_series_raw(asin, parent_obj)
                        if parent_asin and parent_asin != asin:
                            set_series_raw(parent_asin, parent_obj)

                narrator_warnings = compute_narrator_warnings(books_current, asin)
                if settings and getattr(settings, "debug_logging", False):
                    logging.info(f"Computed {len(narrator_warnings)} narrator warnings for {asin}")
                series_col.update_one({"_id": asin}, {"$set": {"narrator_warnings": narrator_warnings}})

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
                new_book_added = bool(new_asins)
                try:
                    raw_changed = _books_raw_changed(old_books, books_current)
                except Exception:
                    raw_changed = False

                # Send notifications when we fetched full data (or had no books before)
                if changed or not old_books:
                    try:
                        self._send_series_notifications(parent_id, doc, old_books, books_current)
                    except Exception:
                        pass

                # A probe is considered to have 'changed' if new audiobooks were discovered or this is the first time we have books
                final_changed = raw_changed or new_book_added or (not old_books and bool(books_current))

                # Schedule next refresh at least one full cycle in the future for both the probed ASIN and its parent
                try:
                    next_when = _now_dt() + _delta_sec(AUTO_REFRESH_CYCLE_SEC)
                    set_series_next_refresh(str(asin), next_when.isoformat() + "Z")
                    if parent_id and parent_id != asin:
                        set_series_next_refresh(str(parent_id), next_when.isoformat() + "Z")
                except Exception:
                    pass
                if job_id:
                    self._finish_job(
                        job_id,
                        {
                            "book_count": len(books_current or []),
                            "changed": bool(final_changed),
                            "new_book": new_book_added,
                        },
                    )
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

    def _release_notifier_run(self):
        """Periodic loop to send release and new-audiobook notifications independent of refresh cadence."""
        import time
        while not self._stop.is_set():
            try:
                self._check_due_release_notifications()
                self._check_new_audiobook_notifications()
                # Prune old job documents once per day to respect settings.max_job_history
                try:
                    _maybe_prune_jobs()
                except Exception:
                    pass
            except Exception:
                pass
            for _ in range(self._release_check_interval_sec):
                if self._stop.is_set():
                    break
                time.sleep(1)

    def _finish_job(self, job_id: str, result: Dict[str, Any]):
        col = get_jobs_collection()
        col.update_one({"_id": ObjectId(job_id)}, {"$set": {"status": "done", "result": result, "finished_at": _now_iso()}}, upsert=True)

    def _record_release_job(self, *, username: str | None, asin: str | None, series_title: str, pending_asins: list[str], body: str, success: bool, error: str | None):
        self._record_notification_job(
            job_type="release_notification",
            username=username,
            asin=asin,
            series_title=series_title,
            pending_asins=pending_asins,
            body=body,
            success=success,
            error=error,
        )

    def _record_notification_job(
        self,
        *,
        job_type: str,
        username: str | None,
        asin: str | None,
        series_title: str,
        pending_asins: list[str],
        body: str,
        success: bool,
        error: str | None,
    ):
        now = _now_iso()
        result: Dict[str, Any] = {
            "notified_asins": list(pending_asins) if pending_asins else [],
            "body": body,
        }
        if error:
            result["error"] = error
        job_doc: Dict[str, Any] = {
            "type": job_type,
            "username": username,
            "asin": asin,
            "title": f"{job_type.replace('_', ' ').title()} for {series_title}",
            "status": "done" if success else "error",
            "result": result,
            "created_at": now,
            "started_at": now,
            "finished_at": now,
        }
        try:
            get_jobs_collection().insert_one(job_doc)
        except Exception:
            pass

    def _check_due_release_notifications(self):
        """Send release notifications when publication time has passed, without waiting for a series refresh."""
        lib_col = get_user_library_collection()
        users_col = get_users_collection()
        series_col = get_series_collection()
        now = _now_dt()

        user_cache: Dict[str, Dict[str, Any]] = {}
        series_cache: Dict[str, Dict[str, Any]] = {}

        filter_q = {"series_asin": {"$exists": True, "$ne": None}}
        count = lib_col.count_documents(filter_q)
        if not count:
            return
        entries_cursor = lib_col.find(filter_q, {"username": 1, "series_asin": 1, "notified_releases": 1, "_id": 1}).batch_size(500)

        # Batch-prefetch distinct usernames and series ASINs to avoid per-entry find_one calls
        try:
            usernames = lib_col.distinct("username", filter_q)
        except Exception:
            usernames = []
        try:
            series_asins = lib_col.distinct("series_asin", filter_q)
        except Exception:
            series_asins = []

        if usernames:
            for u in users_col.find({"username": {"$in": usernames}}, {"username": 1, "notifications": 1}):
                user_cache[u.get("username")] = u or {}

        if series_asins:
            for s in series_col.find({"_id": {"$in": series_asins}}, {"_id": 1, "books": 1, "title": 1, "cover_image": 1}):
                series_cache[s.get("_id")] = s or {}

        def _get_user(username: str) -> Dict[str, Any]:
            return user_cache.get(username) or {}

        def _get_series(asin: str) -> Dict[str, Any]:
            return series_cache.get(asin) or {}

        # Collect UpdateOne operations to batch-write notified state changes
        release_ops: list[UpdateOne] = []
        for entry in entries_cursor:
            username = entry.get("username")
            asin = entry.get("series_asin")
            if not username or not asin:
                continue
            user_doc = _get_user(username)
            notif = user_doc.get("notifications", {}) if isinstance(user_doc, dict) else {}
            enabled = bool(notif.get("enabled", False))
            notify_rel = bool(notif.get("notify_release", False))
            urls = [u for u in notif.get("urls", []) if isinstance(u, str) and u.strip()]
            if not (enabled and notify_rel and urls):
                continue

            series_doc = _get_series(asin)
            books = series_doc.get("books") if isinstance(series_doc, dict) else None
            if not books:
                continue

            now_date = now.date()
            release_candidates = []
            for b in books:
                if not isinstance(b, dict):
                    continue
                b_asin = b.get("asin")
                if not b_asin:
                    continue
                pub_dt = _publication_datetime_utc(b)
                if not pub_dt:
                    continue
                day_diff = abs((now_date - pub_dt.date()).days)
                if day_diff > 1:
                    continue
                # Only include items whose publication datetime is at or before now.
                # Remove the strict upper-bound on elapsed seconds to avoid timing races
                # where a dev write happens just before a sweep but the sweep runs > interval later.
                if pub_dt > now:
                    continue
                release_candidates.append((b, pub_dt))

            if not release_candidates:
                continue

            notified_releases = entry.get("notified_releases", []) if isinstance(entry.get("notified_releases"), list) else []
            pending = [(b, dt) for b, dt in release_candidates if b.get("asin") not in notified_releases]
            if not pending:
                continue

            def _fmt(dt_val: datetime):
                try:
                    return dt_val.replace(microsecond=0).isoformat() + "Z"
                except Exception:
                    return str(dt_val)

            titles = [f"{b.get('title') or b.get('asin')} (released at {_fmt(dt)})" for b, dt in pending]
            pending_asins = [b.get("asin") for b, _ in pending if b.get("asin")]
            series_title = series_doc.get("title") or f"Series {asin}"
            heading = "Audiobook released" if len(pending) == 1 else "Audiobooks released"
            body = f"{heading} in '{series_title}':\n- " + "\n- ".join(titles)
            attachments = [b.get("image_url") for b, _ in pending if b.get("image_url")]

            send_error = None
            try:
                import apprise
                ap = apprise.Apprise()
                for u in urls:
                    ap.add(u)
                try:
                    result = ap.notify(title="Audiobook Release", body=body, attach=attachments)
                    if not result:
                        send_error = "Apprise notification failed (all services returned failure)"
                    else:
                        send_error = None
                except Exception as exc:
                    send_error = str(exc)
            finally:
                self._record_release_job(
                    username=username,
                    asin=asin,
                    series_title=series_title,
                    pending_asins=list(pending_asins),
                    body=body,
                    success=send_error is None,
                    error=send_error,
                )

            if pending_asins:
                try:
                    release_ops.append(UpdateOne({"_id": entry.get("_id")}, {"$addToSet": {"notified_releases": {"$each": pending_asins}}}))
                except Exception:
                    pass

        # Execute batched writes in reasonable-sized chunks
        if release_ops:
            try:
                for i in range(0, len(release_ops), 500):
                    lib_col.bulk_write(release_ops[i : i + 500], ordered=False)
            except Exception:
                pass

    def _check_new_audiobook_notifications(self):
        """Send new-audiobook notifications when new ASINs appear even if the user hasn't triggered a refresh."""
        lib_col = get_user_library_collection()
        users_col = get_users_collection()
        series_col = get_series_collection()

        filter_q = {"series_asin": {"$exists": True, "$ne": None}}
        count = lib_col.count_documents(filter_q)
        if not count:
            return
        entries_cursor = lib_col.find(filter_q, {"username": 1, "series_asin": 1, "notified_new_asins": 1, "notified_releases": 1, "_id": 1}).batch_size(500)

        user_cache: Dict[str, Dict[str, Any]] = {}
        series_cache: Dict[str, Dict[str, Any]] = {}

        # Batch-prefetch distinct usernames and series ASINs to avoid per-entry find_one calls
        try:
            usernames = lib_col.distinct("username", filter_q)
        except Exception:
            usernames = []
        try:
            series_asins = lib_col.distinct("series_asin", filter_q)
        except Exception:
            series_asins = []

        if usernames:
            for u in users_col.find({"username": {"$in": usernames}}, {"username": 1, "notifications": 1}):
                user_cache[u.get("username")] = u or {}

        if series_asins:
            for s in series_col.find({"_id": {"$in": series_asins}}, {"_id": 1, "books": 1, "title": 1, "cover_image": 1}):
                series_cache[s.get("_id")] = s or {}

        def _get_user(username: str) -> Dict[str, Any]:
            return user_cache.get(username) or {}

        def _get_series(asin: str) -> Dict[str, Any]:
            return series_cache.get(asin) or {}

        # Collect UpdateOne operations to batch-write notified state changes
        new_ops: list[UpdateOne] = []
        for entry in entries_cursor:
            username = entry.get("username")
            asin = entry.get("series_asin")
            if not username or not asin:
                continue
            user_doc = _get_user(username)
            notif = user_doc.get("notifications", {}) if isinstance(user_doc, dict) else {}
            enabled = bool(notif.get("enabled", False))
            notify_new = bool(notif.get("notify_new_audiobook", False))
            urls = [u for u in notif.get("urls", []) if isinstance(u, str) and u.strip()]
            if not (enabled and notify_new and urls):
                continue

            series_doc = _get_series(asin)
            books = series_doc.get("books") if isinstance(series_doc, dict) else None
            if not isinstance(books, list) or not books:
                continue

            narrator_warnings = compute_narrator_warnings(books, asin)

            def _is_visible(book: Dict[str, Any] | None) -> bool:
                return bool(book and not is_book_hidden(book))

            visible_books = []
            for b in books:
                if not isinstance(b, dict):
                    continue
                if not _is_visible(b):
                    continue
                visible_books.append(b)
            if not visible_books:
                continue

            book_map: Dict[str, Dict[str, Any]] = {}
            for b in visible_books:
                b_asin = b.get("asin")
                if not b_asin:
                    continue
                book_map[b_asin] = b
            if not book_map:
                continue

            known_asins = entry.get("notified_new_asins") if isinstance(entry.get("notified_new_asins"), list) else []
            known_releases = entry.get("notified_releases") if isinstance(entry.get("notified_releases"), list) else []
            known_set = {a for a in known_asins if a} | {a for a in known_releases if a}
            pending_asins = []
            for b in visible_books:
                if not isinstance(b, dict):
                    continue
                asin_val = b.get("asin")
                if not asin_val or asin_val in known_set:
                    continue
                pending_asins.append(asin_val)

            series_title = series_doc.get("title") or f"Series {asin}"
            initialized = bool(entry.get("notified_new_asins_initialized"))

            if pending_asins and initialized:
                titles = [book_map[a].get("title") or a for a in pending_asins if a in book_map]
                body = f"New audiobooks found in '{series_title}':\n- " + "\n- ".join(titles)
                if narrator_warnings:
                    body += "\n\nNote: Narrator changes detected for this book."
                apprise_error = None
                try:
                    import apprise
                    ap = apprise.Apprise()
                    for u in urls:
                        ap.add(u)
                    try:
                        result = ap.notify(title="New Audiobook(s)", body=body)
                        if not result:
                            apprise_error = "Apprise notification failed (all services returned failure)"
                        else:
                            apprise_error = None
                    except Exception as exc:
                        apprise_error = str(exc)
                finally:
                    self._record_notification_job(
                        job_type="new_audiobook_notification",
                        username=username,
                        asin=asin,
                        series_title=series_title,
                        pending_asins=list(pending_asins),
                        body=body,
                        success=apprise_error is None,
                        error=apprise_error,
                    )

            try:
                entry_id = entry.get("_id")
                if entry_id is not None:
                    new_ops.append(UpdateOne({"_id": entry_id}, {"$set": {"notified_new_asins": list(book_map.keys()), "notified_new_asins_initialized": True}}))
            except Exception:
                pass

        # Flush batched updates
        if new_ops:
            try:
                for i in range(0, len(new_ops), 500):
                    lib_col.bulk_write(new_ops[i : i + 500], ordered=False)
            except Exception:
                pass

    def _send_series_notifications(self, asin: str, series_doc: Dict[str, Any], old_books: list, books_current: list):
        """Notify users tracking this series about new audiobooks and releases based on their settings.
        - New audiobook: when current books contain ASINs not in old_books.
        - Release: when a book's publication_datetime (or release_date fallback) is at/past now (UTC) and user hasn't been notified for that ASIN.
        """
        def _get(book, key):
            return book.get(key) if isinstance(book, dict) else getattr(book, key, None)

        def _is_visible(book):
            return not is_book_hidden(book)

        # Build sets for diff (support dicts or model instances)
        old_asins = {
            _get(b, "asin")
            for b in (old_books or [])
            if _get(b, "asin") and _is_visible(b)
        }
        cur_asins = {
            _get(b, "asin")
            for b in (books_current or [])
            if _get(b, "asin") and _is_visible(b)
        }
        new_asins = [a for a in cur_asins - old_asins if a]
        
        # If this is the first time we have books for this series, bail out (no notifications on initial add)
        if not old_asins:
            return

        # Map asin -> book for quick lookup
        book_map = {}
        for b in (books_current or []):
            if not _is_visible(b):
                continue
            asin_val = _get(b, "asin")
            if asin_val:
                book_map[asin_val] = b

        narrator_warnings = compute_narrator_warnings(books_current, asin)

        # Release candidates: publication_datetime (exact UTC) or release_date fallback
        # Only include books published within ±1 day to avoid stale notifications for months-old releases
        now = _now_dt()
        now_date = now.date()

        release_candidates = []  # list of tuples (book, publication_dt_naive_utc)
        new_asin_set = set(new_asins)
        for b in (books_current or []):
            asin_val = _get(b, "asin")
            if not asin_val or asin_val not in new_asin_set or not _is_visible(b):
                continue
            pub_dt = _publication_datetime_utc(b)
            if pub_dt:
                day_diff = abs((now_date - pub_dt.date()).days)
                if day_diff <= 1 and pub_dt <= now:
                    release_candidates.append((b, pub_dt))

        # Find all users tracking this series
        lib_col = get_user_library_collection()
        users_col = get_users_collection()
        series_title = series_doc.get("title") or f"Series {asin}"

        job_recorded = False
        ops: list[UpdateOne] = []

        # Batch-fetch all users to avoid N+1 queries
        entries = list(lib_col.find({"series_asin": asin}))
        if entries:
            usernames = [e.get("username") for e in entries if e.get("username")]
            user_docs = {doc["username"]: doc for doc in users_col.find({"username": {"$in": usernames}})} if usernames else {}
        else:
            user_docs = {}

        for entry in entries:
            username = entry.get("username")
            if not username:
                continue
            user_doc = user_docs.get(username) or {}
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
                attachments = [book_map[a].get("image_url") for a in to_send if a in book_map and book_map[a].get("image_url")]
                if titles:
                    body = f"New audiobooks found in '{series_title}':\n- " + "\n- ".join(titles)
                    if narrator_warnings:
                        body += "\n\nNote: Narrator changes detected in this series."
                    to_send_msgs.append(("New Audiobook(s)", body, to_send, attachments))

            # Release notifications on the configured release day
            if notify_rel and release_candidates:
                notified_releases = entry.get("notified_releases", []) if isinstance(entry.get("notified_releases"), list) else []
                pending = [(b, dt) for b, dt in release_candidates if _get(b, "asin") not in notified_releases]

                def _fmt(dt_val: datetime):
                    try:
                        return dt_val.replace(microsecond=0).isoformat() + "Z"
                    except Exception:
                        return str(dt_val)

                if pending:
                    titles = [
                        f"{b.get('title') or _get(b, 'asin')} (released at {_fmt(dt)})"
                        for b, dt in pending
                    ]
                    pending_asins = [_get(b, "asin") for b, _ in pending if _get(b, "asin")]
                    attachments = [b.get("image_url") for b, _ in pending if b.get("image_url")]
                    heading = "Audiobook released" if len(pending) == 1 else "Audiobooks released"
                    body = f"{heading} in '{series_title}':\n- " + "\n- ".join(titles)
                    to_send_msgs.append(("Audiobook Release", body, pending_asins, attachments))

            if not to_send_msgs:
                continue

            new_audiobook_msg = next((msg for msg in to_send_msgs if msg[0] == "New Audiobook(s)"), None)
            release_msg = next((msg for msg in to_send_msgs if msg[0] == "Audiobook Release"), None)
            apprise_error = None
            sent_any = False
            # Send via Apprise
            try:
                import apprise
                ap = apprise.Apprise()
                for u in urls:
                    ap.add(u)
                # send each message with attachments
                for msg in to_send_msgs:
                    title, body = msg[0], msg[1]
                    attachments = msg[3] if len(msg) > 3 else []
                    try:
                        result = ap.notify(title=title, body=body, attach=attachments)
                        if not result:
                            apprise_error = "Apprise notification failed (all services returned failure)"
                        else:
                            sent_any = True
                    except Exception as exc:
                        apprise_error = str(exc)
            except Exception as exc:
                apprise_error = str(exc)
            finally:
                # Record job for new audiobook notification
                if new_audiobook_msg:
                    pending_asins_for_job = new_audiobook_msg[2] if len(new_audiobook_msg) > 2 else []
                    self._record_notification_job(
                        job_type="new_audiobook_notification",
                        username=username,
                        asin=asin,
                        series_title=series_title,
                        pending_asins=list(pending_asins_for_job),
                        body=new_audiobook_msg[1],
                        success=apprise_error is None,
                        error=apprise_error,
                    )
                # Record job for release notification
                if release_msg:
                    pending_asins_for_job = release_msg[2] if len(release_msg) > 2 else []
                    self._record_release_job(
                        username=username,
                        asin=asin,
                        series_title=series_title,
                        pending_asins=list(pending_asins_for_job),
                        body=release_msg[1],
                        success=apprise_error is None,
                        error=apprise_error,
                    )
            # Mark that we attempted notifications (return value will expose this)
            if sent_any:
                job_recorded = True

            # Update per-user notification state: releases
            if notify_new and new_asins:
                # Persist new-audiobook notifications to avoid repeat alerts for the same titles/ASINs
                try:
                    to_mark_new = []
                    for msg in to_send_msgs:
                        if msg[0] == "New Audiobook(s)" and len(msg) > 2:
                            to_mark_new.extend([a for a in msg[2] if a])
                        if to_mark_new:
                            try:
                                ops.append(UpdateOne({"_id": entry.get("_id")}, {"$addToSet": {"notified_new_asins": {"$each": to_mark_new}}}))
                            except Exception:
                                pass
                except Exception:
                    pass

            if notify_rel and release_candidates:
                try:
                    to_mark_rel = []
                    for msg in to_send_msgs:
                        if msg[0] == "Audiobook Release" and len(msg) > 2:
                            to_mark_rel.extend(msg[2])
                    if to_mark_rel:
                        try:
                            ops.append(UpdateOne({"_id": entry.get("_id")}, {"$addToSet": {"notified_releases": {"$each": to_mark_rel}}}))
                        except Exception:
                            pass
                except Exception:
                    pass

        # Flush batched per-series notification updates
        if ops:
            try:
                for i in range(0, len(ops), 500):
                    lib_col.bulk_write(ops[i : i + 500], ordered=False)
            except Exception:
                pass

        return job_recorded


worker = TaskWorker()


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

def _now_dt():
    return datetime.now(timezone.utc).replace(tzinfo=None)

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


def _publication_datetime_utc(book: Any) -> datetime | None:
    """Return publication datetime as naive UTC. Falls back to release_date at 00:00 UTC."""
    def _val(key: str):
        if isinstance(book, dict):
            return book.get(key)
        return getattr(book, key, None) if hasattr(book, key) else None

    raw_pub = _val("publication_datetime")
    # If top-level publication_datetime is missing, check raw.publication_datetime set by developer tools
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
    # Fallback to release_date at midnight UTC
    try:
        rd = _val("release_date")
        if rd and isinstance(rd, str):
            ds = rd[:10]
            y, m, d = ds.split("-")
            return datetime(int(y), int(m), int(d))
    except Exception:
        return None
    return None


def _rebalance_auto_refresh(reference: datetime | None = None):
    if reference is None:
        reference = _now_dt()
    series_col = get_series_collection()
    total = series_col.count_documents({})
    if not total:
        return
    # Use a safe fallback in case the module-level constant isn't available at runtime
    cycle_sec = globals().get("AUTO_REFRESH_CYCLE_SEC", 24 * 60 * 60)
    try:
        interval = cycle_sec / total
    except Exception:
        interval = cycle_sec
    # Ensure interval is a numeric value; fall back to cycle_sec on any error
    try:
        interval = float(interval)
        if interval <= 0:
            interval = float(cycle_sec)
    except Exception:
        interval = float(cycle_sec)
    now = reference
    offset_acc = interval
    from datetime import timezone
    datetime_min_utc = datetime.min.replace(tzinfo=timezone.utc)
    # Iterate cursor sorted by fetched_at (missing fetched_at sorts earliest)
    cursor = series_col.find({}, {"_id": 1, "fetched_at": 1}).sort("fetched_at", 1)
    ops: list[UpdateOne] = []
    for doc in cursor:
        offset = max(int(offset_acc), 1)
        target = now + _delta_sec(offset)
        ops.append(UpdateOne({"_id": doc.get("_id")}, {"$set": {"next_refresh_at": target.isoformat() + "Z"}}))
        offset_acc += interval

    if ops:
        try:
            for i in range(0, len(ops), 500):
                series_col.bulk_write(ops[i : i + 500], ordered=False)
        except Exception:
            # Fall back to setting individually if bulk write fails
            for op in ops:
                try:
                    series_col.update_one(op._filter, op._doc)
                except Exception:
                    pass


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


def _maybe_prune_jobs():
    """Prune the jobs collection to keep only the most recent `max_job_history` entries.

    This runs at most once per day (UTC) and is safe to call frequently.
    """
    global _last_jobs_prune_date
    try:
        today = datetime.now(timezone.utc).date()
        if _last_jobs_prune_date == today:
            return
        _last_jobs_prune_date = today
        settings = load_settings()
        max_keep = getattr(settings, "max_job_history", None)
        if not isinstance(max_keep, int) or max_keep <= 0:
            return
        col = get_jobs_collection()
        total = col.count_documents({})
        if total <= max_keep:
            return
        # Get IDs to keep (most recent max_keep jobs)
        jobs_to_keep = [doc["_id"] for doc in col.find({}).sort([("_id", -1)]).limit(max_keep)]
        if jobs_to_keep:
            col.delete_many({"_id": {"$nin": jobs_to_keep}})
        else:
            col.delete_many({})
    except Exception:
        logging.exception("Failed to prune jobs collection")


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
    
    # Count and iterate cursor to avoid loading all series into memory
    count = series_col.count_documents({})
    if not count:
        return {"count": 0, "message": "No series to reschedule"}
    interval_sec = AUTO_REFRESH_CYCLE_SEC
    now = _now_dt()
    cursor = series_col.find({}, {"_id": 1}).batch_size(500)
    ops: list[UpdateOne] = []
    for i, series in enumerate(cursor):
        # Calculate offset for this series across the 24h window
        offset_sec = int((i / count) * interval_sec)
        next_refresh = now + _delta_sec(offset_sec)
        next_refresh_iso = next_refresh.isoformat() + "Z"
        ops.append(UpdateOne({"_id": series["_id"]}, {"$set": {"next_refresh_at": next_refresh_iso}}))

    if ops:
        try:
            for i in range(0, len(ops), 500):
                series_col.bulk_write(ops[i : i + 500], ordered=False)
        except Exception:
            # Fall back to individual updates
            for op in ops:
                try:
                    series_col.update_one(op._filter, op._doc)
                except Exception:
                    pass
    
    return {"count": count, "message": f"Rescheduled {count} series over 24 hours"}


def refresh_all_series(source: str | None = "manual") -> dict:
    """Enqueue a refresh probe for every series and return job ids."""
    series_col = get_series_collection()
    jobs = []
    for doc in series_col.find({}):
        asin = doc.get("_id")
        if not asin:
            continue
        try:
            jid = enqueue_refresh_probe(str(asin), response_groups=None, source=source)
            jobs.append(jid)
        except Exception:
            # Continue on error for robustness
            continue
    return {"count": len(jobs), "job_ids": jobs}


def enqueue_reschedule_all_series(username: str | None = None, delay_seconds: int = 60) -> str:
    """Create a queued job that will run reschedule_all_series after a delay and return the job id."""
    job_id = str(get_jobs_collection().insert_one({
        "type": "reschedule_all_series",
        "username": username,
        "status": "queued",
        "delay_seconds": int(delay_seconds),
        "created_at": _now_iso(),
    }).inserted_id)
    # Enqueue the worker job
    worker.enqueue({"type": "reschedule_all_series", "job_id": job_id, "delay_seconds": int(delay_seconds)})
    return job_id
