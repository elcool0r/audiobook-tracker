import time
from typing import Optional, Dict
from pathlib import Path
from datetime import datetime, timedelta, timezone
import warnings

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r".*'crypt' is deprecated.*",
    module="passlib.utils",
)
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"jose\.jwt",
)
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"datetime\.datetime\.utcnow\(\) is deprecated",
)

from fastapi import Request, HTTPException
from jose import jwt, JWTError
from passlib.context import CryptContext

from .settings import load_settings
from .db import get_users_collection, get_logs_collection

PWD_CTX = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")
SECRET_KEY = None  # overridden by settings on startup
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_SECONDS = 60 * 60 * 24
TOKEN_NAME = "auth_token"


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def log_auth_event(event: str, username: str, ip: str, user_agent: str, details: str = ""):
    logs_col = get_logs_collection()
    logs_col.insert_one({
        "event": event,
        "username": username,
        "ip": ip,
        "user_agent": user_agent,
        "details": details,
        "timestamp": datetime.now(timezone.utc)
    })


def is_account_locked(user_doc):
    lock_until = user_doc.get("lock_until")
    if lock_until:
        now = datetime.now(timezone.utc)
        if isinstance(lock_until, datetime):
            lock_dt = _ensure_utc(lock_until)
            return lock_dt > now
        elif isinstance(lock_until, str):
            try:
                lock_dt = datetime.fromisoformat(lock_until)
                lock_dt = _ensure_utc(lock_dt)
                return lock_dt > now
            except Exception:
                pass
    return False


def record_failed_attempt(username: str):
    users_col = get_users_collection()
    user_doc = users_col.find_one({"username": username})
    if user_doc:
        failed_attempts = user_doc.get("failed_attempts", 0) + 1
        update = {"failed_attempts": failed_attempts}
        if failed_attempts >= 5:
            lock_until = datetime.now(timezone.utc) + timedelta(minutes=15)
            update["lock_until"] = lock_until
        users_col.update_one({"username": username}, {"$set": update})


def record_successful_login(username: str):
    users_col = get_users_collection()
    users_col.update_one({"username": username}, {"$set": {"failed_attempts": 0, "lock_until": None}})


def verify_password(plain: str, hashed: str) -> bool:
    return PWD_CTX.verify(plain, hashed)


def get_password_hash(password: str) -> str:
    return PWD_CTX.hash(password)


def create_access_token(data: Dict[str, str], expires_delta: Optional[int] = None) -> str:
    to_encode = data.copy()
    now = int(time.time())
    if expires_delta is None:
        expires_delta = ACCESS_TOKEN_EXPIRE_SECONDS
    to_encode.update({"exp": now + int(expires_delta), "iat": now})
    settings = load_settings()
    key = settings.secret_key
    if not key:
        raise ValueError("SECRET_KEY not set in settings")
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message=r"datetime\.datetime\.utcnow\(\) is deprecated",
        )
        token = jwt.encode(to_encode, key, algorithm=ALGORITHM)
    return token


async def get_current_user(request: Request):
    token = request.cookies.get(TOKEN_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        settings = load_settings()
        key = settings.secret_key
        if not key:
            raise ValueError("SECRET_KEY not set in settings")
        payload = jwt.decode(token, key, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
        user_doc = get_users_collection().find_one({"username": username})
        if not user_doc:
            raise HTTPException(status_code=401, detail="User not found")
        return {
            "username": username,
            "role": user_doc.get("role", "user"),
            "date_format": user_doc.get("date_format", "iso"),
            "frontpage_slug": user_doc.get("frontpage_slug") or username,
        }
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
