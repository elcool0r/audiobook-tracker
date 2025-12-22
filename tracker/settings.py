from pydantic import BaseModel
from typing import Optional
import os

from .db import get_settings_collection, get_users_collection
from .security import get_password_hash


class Settings(BaseModel):
    response_groups: Optional[str] = None
    rate_rps: float = 2.0
    secret_key: Optional[str] = None
    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None
    proxy_enabled: bool = False
    max_job_history: int = 100
    auto_refresh_enabled: bool = True
    manual_refresh_interval_minutes: int = 10
    user_agent: Optional[str] = None
    allow_non_admin_series_search: bool = True
    skip_known_series_search: bool = True
    default_frontpage_slug: Optional[str] = None
    log_retention_days: int = 30
    debug_logging: bool = False


def default_settings() -> Settings:
    return Settings(
        response_groups=None,
        rate_rps=2.0,
        secret_key=None,
        proxy_url=None,
        proxy_username=None,
        proxy_password=None,
        proxy_enabled=False,
        max_job_history=100,
        auto_refresh_enabled=True,
        manual_refresh_interval_minutes=10,
        user_agent=None,
        allow_non_admin_series_search=True,
        skip_known_series_search=True,
        default_frontpage_slug=None,
        log_retention_days=30,
        debug_logging=False,
    )


def ensure_default_admin():
    users = get_users_collection()
    if users.count_documents({}) == 0:
        users.insert_one({
            "username": "admin",
            "password_hash": get_password_hash("admin"),
            "role": "admin",
            "date_format": "iso",
        })
    # Migrate any legacy 'superadmin' roles to 'admin'
    try:
        users.update_many({"role": "superadmin"}, {"$set": {"role": "admin"}})
    except Exception:
        pass


def load_settings() -> Settings:
    col = get_settings_collection()
    doc = col.find_one({"_id": "global"})
    if not doc:
        s = default_settings()
        # Override with env vars
        if os.getenv("SECRET_KEY"):
            s.secret_key = os.getenv("SECRET_KEY")
        col.insert_one({"_id": "global", **s.dict()})
        ensure_default_admin()
        return s
    settings_obj = Settings.parse_obj({k: v for k, v in doc.items() if k != "_id"})
    # Override with env vars if set
    if os.getenv("SECRET_KEY"):
        settings_obj.secret_key = os.getenv("SECRET_KEY")
    ensure_default_admin()
    return settings_obj


def save_settings(s: Settings) -> None:
    col = get_settings_collection()
    col.update_one({"_id": "global"}, {"$set": s.dict()}, upsert=True)
