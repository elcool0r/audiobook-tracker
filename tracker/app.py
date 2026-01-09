from pathlib import Path
from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import Response
from fastapi.exception_handlers import http_exception_handler
from contextlib import asynccontextmanager
import logging
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
# from fastapi_csrf_protect import CsrfProtect
# from fastapi_csrf_protect.exceptions import CsrfProtectError
import datetime
from bson import ObjectId
from datetime import datetime as _dt, timezone
import math
from typing import Optional, Dict, Any
from .db import get_series_collection
from .frontpage import render_frontpage_for_slug, _get_publication_dt


def _format_time_left(release_dt: _dt, now: _dt) -> tuple[str, int | None, int | None]:
    # Delegate to shared helper to avoid duplicate logic and keep a shim for tests
    from .app_helpers import format_time_left as _fmt
    return _fmt(release_dt, now)


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
from . import settings as settings_mod
from .__version__ import __version__
from .tasks import worker
from .app_helpers import (
    parse_date,
    format_dt,
    format_d,
    format_runtime,
    preload_series_data,
    compute_num_latest,
)
from prometheus_client import Gauge, Counter, generate_latest, REGISTRY


def _get_or_create_metric(name, ctor, *args, **kwargs):
    try:
        return ctor(name, *args, **kwargs)
    except ValueError:
        # Metric already registered; return existing collector if present
        try:
            return REGISTRY._names_to_collectors.get(name)
        except Exception:
            return None

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
series_count = _get_or_create_metric('audiobook_series_total', Gauge, 'Total number of series')
user_count = _get_or_create_metric('audiobook_users_total', Gauge, 'Total number of users')
login_attempts = _get_or_create_metric('audiobook_login_attempts_total', Counter, 'Total login attempts', ['status'])
failed_logins = _get_or_create_metric('audiobook_failed_logins_total', Counter, 'Total failed logins')

def _p(path: str) -> str:
    """Prefix a route path with the configured base."""
    return f"{BASE_PATH}{path}"

async def get_admin_user(request: Request):
    user = await get_current_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    return user

async def _start_worker():
    settings_mod.ensure_default_admin()
    ensure_indexes()
    rebuild_series_user_counts()
    # Cleanup old logs
    settings = settings_mod.load_settings()
    cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=settings.log_retention_days)
    from .db import get_logs_collection
    logs_col = get_logs_collection()
    logs_col.delete_many({"timestamp": {"$lt": cutoff}})
    worker.start()

async def _stop_worker():
    worker.stop()

def create_app() -> FastAPI:
    settings = settings_mod.load_settings()
    if getattr(settings, "debug_logging", False):
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        await _start_worker()
        yield
        # Shutdown
        await _stop_worker()

    app = FastAPI(
        docs_url=_p("/docs"),
        redoc_url=_p("/redoc"),
        openapi_url=_p("/openapi.json"),
        lifespan=lifespan
    )
    app.state.limiter = limiter
    app.add_middleware(SlowAPIMiddleware)
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    templates.env.globals["base_path"] = BASE_PATH

    app.mount(_p("/static"), StaticFiles(directory=str(BASE_DIR / "static")), name="static")
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="public_static")

    # frontpage rendering moved to tracker.frontpage.render_frontpage_for_slug

    @app.get("/", response_class=HTMLResponse)
    async def public_root(request: Request):
        settings = settings_mod.load_settings()
        slug = (settings.default_frontpage_slug or "").strip()
        page = render_frontpage_for_slug(request, slug, templates)
        if page:
            return page
        return RedirectResponse(url=_p("/"), status_code=302)

    @app.get(_p("/"), response_class=HTMLResponse)
    async def config_root(request: Request):
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("login.html", {"request": request, "settings": settings, "error": None, "version": __version__})

    @app.get(_p("/login"), response_class=HTMLResponse)
    async def login_get(request: Request):  # , csrf_protect: CsrfProtect = Depends()):
        # csrf_token = csrf_protect.generate_csrf()
        settings = settings_mod.load_settings()
        resp = templates.TemplateResponse("login.html", {"request": request, "settings": settings, "error": None, "version": __version__})  # , "csrf_token": csrf_token})
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
            settings_mod.ensure_default_admin()
            user_doc = users.find_one({"username": username})
        if not user_doc:
            log_auth_event("login_failed", username, request.client.host, request.headers.get("user-agent", ""), "User not found")
            login_attempts.labels(status="failed").inc()
            failed_logins.inc()
            settings = settings_mod.load_settings()
            return templates.TemplateResponse("login.html", {"request": request, "settings": settings, "error": "Invalid credentials", "version": __version__})
        if is_account_locked(user_doc):
            log_auth_event("login_failed", username, request.client.host, request.headers.get("user-agent", ""), "Account locked")
            login_attempts.labels(status="failed").inc()
            failed_logins.inc()
            settings = settings_mod.load_settings()
            return templates.TemplateResponse("login.html", {"request": request, "settings": settings, "error": "Account locked due to too many failed attempts", "version": __version__})
        if not verify_password(password, user_doc.get("password_hash", "")):
            record_failed_attempt(username)
            log_auth_event("login_failed", username, request.client.host, request.headers.get("user-agent", ""), "Invalid password")
            logger.warning(f"Failed login attempt for username: {username}")
            login_attempts.labels(status="failed").inc()
            failed_logins.inc()
            settings = settings_mod.load_settings()
            return templates.TemplateResponse("login.html", {"request": request, "settings": settings, "error": "Invalid credentials", "version": __version__})
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
    async def logout(request: Request):
        from .auth import log_auth_event, SECRET_KEY, ALGORITHM
        username = "unknown"
        token = request.cookies.get(TOKEN_NAME)
        if token:
            try:
                from jose import jwt
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                username = payload.get("sub", "unknown")
            except Exception as e:
                logger.warning(
                    "Failed to decode JWT during logout from %s: %s",
                    request.client.host if request.client else "unknown",
                    str(e),
                )
        log_auth_event("logout", username, request.client.host, request.headers.get("user-agent", ""))
        resp = RedirectResponse(url=_p("/login"), status_code=302)
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
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("settings.html", {"request": request, "settings": settings, "user": user, "version": __version__})

    # Chrome DevTools and some extensions probe this path; return 204 to silence 404 noise
    @app.get("/.well-known/appspecific/com.chrome.devtools.json")
    async def _chrome_devtools_probe():
        return Response(status_code=204)

    @app.get(_p("/library"), response_class=HTMLResponse)
    async def library_page(request: Request, user=Depends(get_current_user)):
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("library.html", {"request": request, "user": user, "settings": settings, "version": __version__})

    @app.get("/home/{slug}", response_class=HTMLResponse)
    async def user_home_page(request: Request, slug: str):
        page = render_frontpage_for_slug(request, slug, templates)
        if page:
            return page
        settings = settings_mod.load_settings()
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "settings": settings, "error": "User not found", "version": __version__},
            status_code=404,
        )

    

    @app.get(_p("/series/{asin}"), response_class=HTMLResponse)
    async def view_series_page(request: Request, asin: str):
        # public view for a series by ASIN with public navbar
        from .db import get_series_collection
        series_col = get_series_collection()
        series_doc = series_col.find_one({"$or": [{"asin": asin}, {"_id": asin}]})
        if not series_doc:
            raise HTTPException(status_code=404, detail="Series not found")
        return templates.TemplateResponse(
            "view_series.html",
            {"request": request, "asin": asin, "public_nav": True, "brand_title": "Audiobook Tracker", "version": __version__},
        )

    @app.get(_p("/series-books"), response_class=HTMLResponse)
    async def series_books_page(request: Request, user=Depends(get_current_user)):
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("series_books.html", {"request": request, "user": user, "settings": settings, "version": __version__})

    @app.get(_p("/users"), response_class=HTMLResponse)
    async def users_page(request: Request, user=Depends(get_admin_user)):
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("users.html", {"request": request, "user": user, "settings": settings, "version": __version__})

    @app.get(_p("/profile"), response_class=HTMLResponse)
    async def profile_page(request: Request, user=Depends(get_current_user)):
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("profile.html", {"request": request, "user": user, "settings": settings, "version": __version__})

    @app.get(_p("/series-admin"), response_class=HTMLResponse)
    async def series_admin_page(request: Request, user=Depends(get_admin_user)):
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("series_admin.html", {"request": request, "user": user, "settings": settings, "version": __version__})

    @app.get(_p("/jobs"), response_class=HTMLResponse)
    async def jobs_page(request: Request, user=Depends(get_admin_user)):
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("jobs.html", {"request": request, "user": user, "settings": settings, "version": __version__})

    @app.get(_p("/logs"), response_class=HTMLResponse)
    async def logs_page(request: Request, user=Depends(get_admin_user)):
        from .db import get_logs_collection
        logs_col = get_logs_collection()
        logs = list(logs_col.find().sort("timestamp", -1).limit(100))
        # Convert ObjectId and datetime to string for JSON serialization
        logs = [convert_for_json(log) for log in logs]
        settings = settings_mod.load_settings()
        return templates.TemplateResponse("logs.html", {"request": request, "user": user, "logs": logs, "settings": settings, "version": __version__})

    @app.get("/metrics")
    async def metrics():
        series_count.set(get_series_collection().count_documents({}))
        user_count.set(get_users_collection().count_documents({}))
        return Response(generate_latest(), media_type="text/plain")

    app.include_router(api_router, prefix=_p("/api"))

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("tracker.app:app", host="127.0.0.1", port=8000, reload=True)
