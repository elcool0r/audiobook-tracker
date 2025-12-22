from pathlib import Path
from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import Response
from fastapi.exception_handlers import http_exception_handler
import logging
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
# from fastapi_csrf_protect import CsrfProtect
# from fastapi_csrf_protect.exceptions import CsrfProtectError
import datetime
from bson import ObjectId

from .auth import get_current_user, verify_password, create_access_token, TOKEN_NAME
from .db import get_users_collection, get_series_collection
from .api import api_router
from .library import ensure_indexes, rebuild_series_user_counts, visible_books


def convert_for_json(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_for_json(item) for item in obj]
    else:
        return obj
from .settings import load_settings, ensure_default_admin
from .tasks import worker
from prometheus_client import Gauge, Counter, generate_latest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)

# @CsrfProtect.load_config
# def get_csrf_config():
#     settings = load_settings()
#     return [("secret_key", settings.secret_key or "fallback"), ("max_age", 3600)]

BASE_DIR = Path(__file__).resolve().parent
# Default base path for the tracker UI/API. Keep at /config for compatibility with existing setups.
BASE_PATH = "/config"

# Prometheus metrics
series_count = Gauge('audiobook_series_total', 'Total number of series')
user_count = Gauge('audiobook_users_total', 'Total number of users')
login_attempts = Counter('audiobook_login_attempts_total', 'Total login attempts', ['status'])
failed_logins = Counter('audiobook_failed_logins_total', 'Total failed logins')

def _p(path: str) -> str:
    """Prefix a route path with the configured base."""
    return f"{BASE_PATH}{path}"

async def get_admin_user(request: Request):
    user = await get_current_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    return user

def create_app() -> FastAPI:
    from .settings import load_settings
    settings = load_settings()
    if settings.debug_logging:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    app = FastAPI(
        docs_url=_p("/docs"),
        redoc_url=_p("/redoc"),
        openapi_url=_p("/openapi.json"),
    )
    app.state.limiter = limiter
    app.add_middleware(SlowAPIMiddleware)
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    templates.env.globals["base_path"] = BASE_PATH

    app.mount(_p("/static"), StaticFiles(directory=str(BASE_DIR / "static")), name="static")
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="public_static")

    def render_frontpage_for_slug(request: Request, slug: str):
        if not slug:
            return None
        from .db import get_users_collection
        from .library import get_user_library
        users_col = get_users_collection()
        user_doc = users_col.find_one({"$or": [{"frontpage_slug": slug}, {"username": slug}]})
        if not user_doc:
            return None
        username = user_doc.get("username")
        date_format = user_doc.get("date_format", "iso")
        library = get_user_library(username)

        from datetime import datetime

        def _parse_date(s):
            try:
                return datetime.fromisoformat((s or "").split("T")[0])
            except Exception:
                return None

        def _format_dt(dt: datetime | None):
            if not dt:
                return "—"
            def pad(n):
                return str(n).zfill(2)
            if date_format == "de":
                return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
            if date_format == "us":
                return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
            return f"{dt.date().isoformat()} {pad(dt.hour)}:{pad(dt.minute)}"

        def _format_d(dt: datetime | None):
            if not dt:
                return "—"
            def pad(n):
                return str(n).zfill(2)
            if date_format == "de":
                return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year}"
            if date_format == "us":
                return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year}"
            return dt.date().isoformat()

        def _format_runtime(val) -> str | None:
            try:
                m = int(val or 0)
            except Exception:
                return None
            if m <= 0:
                return None
            h = m // 60
            mins = m % 60
            return f"{h}h {mins}m" if h else f"{mins}m"

        now = datetime.utcnow()
        upcoming_cards = []
        latest_cards = []
        series_rows = []
        total_books = 0
        last_refresh_dt = None

        for it in library:
            books = it.books if isinstance(it.books, list) else []
            visible = visible_books(books)
            total_books += len(visible)
            if it.fetched_at:
                dt = _parse_date(it.fetched_at)
                if dt and (not last_refresh_dt or dt > last_refresh_dt):
                    last_refresh_dt = dt
            series_last_release = None
            for b in visible:
                rd = _parse_date(getattr(b, "release_date", None))
                if not rd:
                    continue
                if rd <= now and (not series_last_release or rd > series_last_release):
                    series_last_release = rd
                book_url = getattr(b, "url", None)
                if not book_url and getattr(b, "asin", None):
                    book_url = f"https://www.audible.com/pd/{getattr(b, 'asin', '')}"
                if rd > now:
                    days = (rd - now).days + (1 if (rd - now).seconds > 0 else 0)
                    runtime_str = _format_runtime(getattr(b, "runtime", None))
                    upcoming_cards.append({
                        "title": getattr(b, "title", None) or it.title,
                        "series": it.title,
                        "narrators": getattr(b, "narrators", None) or "",
                        "runtime": getattr(b, "runtime", None) or "",
                        "runtime_str": runtime_str,
                        "release_dt": rd,
                        "release_str": _format_d(rd),
                        "days_left": days,
                        "image": getattr(b, "image", None),
                        "url": book_url,
                    })
                else:
                    days_ago = (now - rd).days
                    runtime_str = _format_runtime(getattr(b, "runtime", None))
                    latest_cards.append({
                        "title": getattr(b, "title", None) or it.title,
                        "series": it.title,
                        "narrators": getattr(b, "narrators", None) or "",
                        "runtime": getattr(b, "runtime", None) or "",
                        "runtime_str": runtime_str,
                        "release_dt": rd,
                        "release_str": _format_d(rd),
                        "days_ago": days_ago,
                        "image": getattr(b, "image", None),
                        "url": book_url,
                    })
            narr_set = set()
            runtime_mins = 0
            for b in visible:
                if getattr(b, "narrators", None):
                    for n in str(getattr(b, "narrators", "")).split(","):
                        n = n.strip()
                        if n:
                            narr_set.add(n)
                try:
                    runtime_mins += int(getattr(b, "runtime", None) or 0)
                except Exception:
                    pass
            hours = runtime_mins // 60
            mins = runtime_mins % 60
            runtime_str = f"{hours}h {mins}m" if hours else f"{mins}m"
            cover = None
            for b in visible:
                if getattr(b, "image", None):
                    cover = getattr(b, "image", None)
                    break
            if not cover:
                for b in books:
                    if getattr(b, "image", None):
                        cover = getattr(b, "image", None)
                        break
            last_release_str = _format_d(series_last_release)
            last_release_ts = series_last_release.isoformat() if series_last_release else None
            series_rows.append({
                "title": it.title,
                "asin": it.asin,
                "narrators": ", ".join(sorted(narr_set)),
                "book_count": len(visible),
                "runtime": runtime_str,
                "cover": cover,
                "last_release": last_release_str,
                "last_release_ts": last_release_ts,
                "duration_minutes": runtime_mins,
                "url": it.url,
            })

        upcoming_cards.sort(key=lambda x: x["release_dt"])
        latest_cards.sort(key=lambda x: x["release_dt"], reverse=True)
        latest_cards = latest_cards[:4]
        series_rows.sort(key=lambda x: (x["title"] or ""))

        stats = {
            "series_count": len(library),
            "books_count": total_books,
            "last_refresh": _format_dt(last_refresh_dt),
            "slug": user_doc.get("frontpage_slug") or username,
            "username": username,
        }

        return templates.TemplateResponse(
            "frontpage.html",
            {
                "request": request,
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
            },
        )

    @app.get("/", response_class=HTMLResponse)
    async def public_root(request: Request):
        settings = load_settings()
        slug = (settings.default_frontpage_slug or "").strip()
        page = render_frontpage_for_slug(request, slug)
        if page:
            return page
        return templates.TemplateResponse("login.html", {"request": request, "error": None})

    @app.get(_p("/"), response_class=HTMLResponse)
    async def config_root(request: Request):
        return templates.TemplateResponse("login.html", {"request": request, "error": None})

    @app.get(_p("/login"), response_class=HTMLResponse)
    async def login_get(request: Request):  # , csrf_protect: CsrfProtect = Depends()):
        # csrf_token = csrf_protect.generate_csrf()
        resp = templates.TemplateResponse("login.html", {"request": request, "error": None})  # , "csrf_token": csrf_token})
        # csrf_protect.set_csrf_cookie(resp)
        return resp

    @app.post(_p("/login"), response_class=HTMLResponse)
    @limiter.limit("5/minute")
    async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):  # , csrf_protect: CsrfProtect = Depends()):
        # try:
        #     csrf_protect.validate_csrf(request)
        # except CsrfProtectError:
        #     return templates.TemplateResponse("login.html", {"request": request, "error": "CSRF token invalid"})
        from .auth import log_auth_event, is_account_locked, record_failed_attempt, record_successful_login
        users = get_users_collection()
        user_doc = users.find_one({"username": username})
        if not user_doc:
            ensure_default_admin()
            user_doc = users.find_one({"username": username})
        if not user_doc:
            log_auth_event("login_failed", username, request.client.host, request.headers.get("user-agent", ""), "User not found")
            login_attempts.labels(status="failed").inc()
            failed_logins.inc()
            return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})
        if is_account_locked(user_doc):
            log_auth_event("login_failed", username, request.client.host, request.headers.get("user-agent", ""), "Account locked")
            login_attempts.labels(status="failed").inc()
            failed_logins.inc()
            return templates.TemplateResponse("login.html", {"request": request, "error": "Account locked due to too many failed attempts"})
        if not verify_password(password, user_doc.get("password_hash", "")):
            record_failed_attempt(username)
            log_auth_event("login_failed", username, request.client.host, request.headers.get("user-agent", ""), "Invalid password")
            logger.warning(f"Failed login attempt for username: {username}")
            login_attempts.labels(status="failed").inc()
            failed_logins.inc()
            return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})
        record_successful_login(username)
        token = create_access_token({"sub": username})
        log_auth_event("login_success", username, request.client.host, request.headers.get("user-agent", ""))
        logger.info(f"Successful login for username: {username}")
        login_attempts.labels(status="success").inc()
        resp = RedirectResponse(url=_p("/library"), status_code=302)
        secure = request.url.scheme == "https"
        resp.set_cookie(TOKEN_NAME, token, httponly=True, secure=secure)
        return resp

    @app.get(_p("/logout"))
    async def logout(request: Request, user=Depends(get_current_user)):
        from .auth import log_auth_event
        log_auth_event("logout", user["username"], request.client.host, request.headers.get("user-agent", ""))
        resp = RedirectResponse(url=_p("/login"))
        resp.delete_cookie(TOKEN_NAME)
        return resp

    @app.exception_handler(HTTPException)
    async def invalid_token_redirect(request: Request, exc: HTTPException):
        # Redirect browser navigation to login on invalid token; keep API JSON responses unchanged
        if exc.status_code == 401 and str(exc.detail) == "Invalid token" and not request.url.path.startswith(_p("/api")):
            return RedirectResponse(url=_p("/login"), status_code=302)
        return await http_exception_handler(request, exc)

    # Dashboard view removed; library is the default landing page

    @app.get(_p("/settings"), response_class=HTMLResponse)
    async def settings_get(request: Request, user=Depends(get_current_user)):
        settings = load_settings()
        return templates.TemplateResponse("settings.html", {"request": request, "settings": settings, "user": user})

    @app.get(_p("/library"), response_class=HTMLResponse)
    async def library_page(request: Request, user=Depends(get_current_user)):
        settings = load_settings()
        return templates.TemplateResponse("library.html", {"request": request, "user": user, "settings": settings})

    @app.get("/home/{slug}", response_class=HTMLResponse)
    async def user_home_page(request: Request, slug: str):
        # Fully server-rendered frontpage: Upcoming, Latest, and Series (no series view links)
        from .db import get_users_collection
        from .library import get_user_library
        users_col = get_users_collection()
        user_doc = users_col.find_one({"$or": [{"frontpage_slug": slug}, {"username": slug}]})
        if not user_doc:
            return templates.TemplateResponse("login.html", {"request": request, "error": "User not found"}, status_code=404)
        username = user_doc.get("username")
        date_format = user_doc.get("date_format", "iso")
        library = get_user_library(username)

        from datetime import datetime

        def _parse_date(s):
            try:
                return datetime.fromisoformat((s or "").split("T")[0])
            except Exception:
                return None

        def _format_dt(dt: datetime | None):
            if not dt:
                return "—"
            def pad(n):
                return str(n).zfill(2)
            if date_format == "de":
                return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
            if date_format == "us":
                return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
            return f"{dt.date().isoformat()} {pad(dt.hour)}:{pad(dt.minute)}"

        def _format_d(dt: datetime | None):
            if not dt:
                return "—"
            def pad(n):
                return str(n).zfill(2)
            if date_format == "de":
                return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year}"
            if date_format == "us":
                return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year}"
            return dt.date().isoformat()

        def _format_runtime(val) -> str | None:
            try:
                m = int(val or 0)
            except Exception:
                return None
            if m <= 0:
                return None
            h = m // 60
            mins = m % 60
            return f"{h}h {mins}m" if h else f"{mins}m"

        now = datetime.utcnow()
        upcoming_cards = []
        latest_cards = []
        series_rows = []
        total_books = 0
        last_refresh_dt = None

        for it in library:
            books = it.books if isinstance(it.books, list) else []
            visible = visible_books(books)
            total_books += len(visible)
            if it.fetched_at:
                dt = _parse_date(it.fetched_at)
                if dt and (not last_refresh_dt or dt > last_refresh_dt):
                    last_refresh_dt = dt
            series_last_release = None
            for b in visible:
                rd = _parse_date(getattr(b, "release_date", None))
                if not rd:
                    continue
                if rd <= now and (not series_last_release or rd > series_last_release):
                    series_last_release = rd
                book_url = getattr(b, "url", None)
                if not book_url and getattr(b, "asin", None):
                    book_url = f"https://www.audible.com/pd/{getattr(b, 'asin', '')}"
                if rd > now:
                    days = (rd - now).days + (1 if (rd - now).seconds > 0 else 0)
                    runtime_str = _format_runtime(getattr(b, "runtime", None))
                    upcoming_cards.append({
                        "title": getattr(b, "title", None) or it.title,
                        "series": it.title,
                        "narrators": getattr(b, "narrators", None) or "",
                        "runtime": getattr(b, "runtime", None) or "",
                        "runtime_str": runtime_str,
                        "release_dt": rd,
                        "release_str": _format_d(rd),
                        "days_left": days,
                        "image": getattr(b, "image", None),
                        "url": book_url,
                    })
                else:
                    days_ago = (now - rd).days
                    runtime_str = _format_runtime(getattr(b, "runtime", None))
                    latest_cards.append({
                        "title": getattr(b, "title", None) or it.title,
                        "series": it.title,
                        "narrators": getattr(b, "narrators", None) or "",
                        "runtime": getattr(b, "runtime", None) or "",
                        "runtime_str": runtime_str,
                        "release_dt": rd,
                        "release_str": _format_d(rd),
                        "days_ago": days_ago,
                        "image": getattr(b, "image", None),
                        "url": book_url,
                    })
            narr_set = set()
            runtime_mins = 0
            for b in visible:
                if getattr(b, "narrators", None):
                    for n in str(getattr(b, "narrators", "")).split(","):
                        n = n.strip()
                        if n:
                            narr_set.add(n)
                try:
                    runtime_mins += int(getattr(b, "runtime", None) or 0)
                except Exception:
                    pass
            hours = runtime_mins // 60
            mins = runtime_mins % 60
            runtime_str = f"{hours}h {mins}m" if hours else f"{mins}m"
            cover = None
            for b in visible:
                if getattr(b, "image", None):
                    cover = getattr(b, "image", None)
                    break
            if not cover:
                for b in books:
                    if getattr(b, "image", None):
                        cover = getattr(b, "image", None)
                        break
            last_release_str = _format_d(series_last_release)
            last_release_ts = series_last_release.isoformat() if series_last_release else None
            series_rows.append({
                "title": it.title,
                "asin": it.asin,
                "narrators": ", ".join(sorted(narr_set)),
                "book_count": len(visible),
                "runtime": runtime_str,
                "cover": cover,
                "last_release": last_release_str,
                "last_release_ts": last_release_ts,
                "duration_minutes": runtime_mins,
                "url": it.url,
            })

        upcoming_cards.sort(key=lambda x: x["release_dt"])
        latest_cards.sort(key=lambda x: x["release_dt"], reverse=True)
        latest_cards = latest_cards[:4]
        series_rows.sort(key=lambda x: (x["title"] or ""))

        stats = {
            "series_count": len(library),
            "books_count": total_books,
            "last_refresh": _format_dt(last_refresh_dt),
            "slug": user_doc.get("frontpage_slug") or username,
            "username": username,
        }

        return templates.TemplateResponse(
            "frontpage.html",
            {
                "request": request,
                "public_nav": True,
                "brand_title": "Audiobook Tracker",
                "hide_nav": True,
                "page_title": "Audiobook Tracker",
                "stats": stats,
                "upcoming": upcoming_cards,
                "latest": latest_cards,
                "series": series_rows,
            },
        )

    

    @app.get(_p("/series/{asin}"), response_class=HTMLResponse)
    async def view_series_page(request: Request, asin: str):
        # public view for a series by ASIN with public navbar
        return templates.TemplateResponse(
            "view_series.html",
            {"request": request, "asin": asin, "public_nav": True, "brand_title": "Audiobook Tracker"},
        )

    @app.get(_p("/series-books"), response_class=HTMLResponse)
    async def series_books_page(request: Request, user=Depends(get_current_user)):
        return templates.TemplateResponse("series_books.html", {"request": request, "user": user})

    @app.get(_p("/users"), response_class=HTMLResponse)
    async def users_page(request: Request, user=Depends(get_admin_user)):
        return templates.TemplateResponse("users.html", {"request": request, "user": user})

    @app.get(_p("/profile"), response_class=HTMLResponse)
    async def profile_page(request: Request, user=Depends(get_current_user)):
        return templates.TemplateResponse("profile.html", {"request": request, "user": user})

    @app.get(_p("/series-admin"), response_class=HTMLResponse)
    async def series_admin_page(request: Request, user=Depends(get_admin_user)):
        return templates.TemplateResponse("series_admin.html", {"request": request, "user": user})

    @app.get(_p("/jobs"), response_class=HTMLResponse)
    async def jobs_page(request: Request, user=Depends(get_admin_user)):
        return templates.TemplateResponse("jobs.html", {"request": request, "user": user})

    @app.get(_p("/logs"), response_class=HTMLResponse)
    async def logs_page(request: Request, user=Depends(get_admin_user)):
        from .db import get_logs_collection
        logs_col = get_logs_collection()
        logs = list(logs_col.find().sort("timestamp", -1).limit(100))
        # Convert ObjectId and datetime to string for JSON serialization
        logs = [convert_for_json(log) for log in logs]
        return templates.TemplateResponse("logs.html", {"request": request, "user": user, "logs": logs})

    @app.get("/metrics")
    async def metrics():
        series_count.set(get_series_collection().count_documents({}))
        user_count.set(get_users_collection().count_documents({}))
        return Response(generate_latest(), media_type="text/plain")

    app.include_router(api_router, prefix=_p("/api"))

    @app.on_event("startup")
    async def _start_worker():
        ensure_default_admin()
        ensure_indexes()
        rebuild_series_user_counts()
        # Cleanup old logs
        settings = load_settings()
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=settings.log_retention_days)
        from .db import get_logs_collection
        logs_col = get_logs_collection()
        logs_col.delete_many({"timestamp": {"$lt": cutoff}})
        worker.start()

    @app.on_event("shutdown")
    async def _stop_worker():
        worker.stop()

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("tracker.app:app", host="127.0.0.1", port=8000, reload=True)
