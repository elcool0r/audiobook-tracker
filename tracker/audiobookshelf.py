from __future__ import annotations

from dataclasses import dataclass
import hashlib
import threading
import time
from typing import Any, Iterable
from urllib.parse import quote, urlsplit, urlunsplit

import requests


REQUEST_TIMEOUT_SECONDS = 10
SERIES_CACHE_TTL_SECONDS = 300


class AudiobookshelfError(RuntimeError):
    """Safe, user-facing Audiobookshelf integration error."""


def normalize_host(value: str) -> str:
    host = (value or "").strip().rstrip("/")
    if not host:
        raise AudiobookshelfError("Audiobookshelf host is required")
    parsed = urlsplit(host)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise AudiobookshelfError("Audiobookshelf host must be a valid HTTP or HTTPS URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise AudiobookshelfError("Audiobookshelf host must not contain credentials, a query, or a fragment")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def normalize_series_title(value: str) -> str:
    title = " ".join((value or "").casefold().split())
    if title.endswith(" series"):
        title = title[:-7].rstrip()
    normalized = "".join(char if char.isalnum() else " " for char in title)
    return " ".join(normalized.split())


@dataclass(frozen=True)
class AudiobookshelfClient:
    host: str
    api_token: str
    timeout: int = REQUEST_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", normalize_host(self.host))
        if not (self.api_token or "").strip():
            raise AudiobookshelfError("Audiobookshelf API token is required")

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        try:
            response = requests.get(
                f"{self.host}{path}",
                headers={"Authorization": f"Bearer {self.api_token}"},
                params=params,
                timeout=self.timeout,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise AudiobookshelfError(f"Could not connect to Audiobookshelf: {exc}") from exc
        if response.is_redirect:
            raise AudiobookshelfError("Audiobookshelf returned an unexpected redirect")
        if response.status_code in {401, 403}:
            raise AudiobookshelfError("Audiobookshelf rejected the API token")
        if not response.ok:
            raise AudiobookshelfError(f"Audiobookshelf returned HTTP {response.status_code}")
        try:
            return response.json()
        except ValueError as exc:
            raise AudiobookshelfError("Audiobookshelf returned invalid JSON") from exc

    def get_libraries(self) -> list[dict[str, str]]:
        payload = self._get("/api/libraries")
        libraries = payload.get("libraries", []) if isinstance(payload, dict) else []
        result = []
        for library in libraries:
            if not isinstance(library, dict) or library.get("mediaType") != "book":
                continue
            library_id = str(library.get("id") or "").strip()
            name = str(library.get("name") or "").strip()
            if library_id and name:
                result.append({"id": library_id, "name": name})
        return sorted(result, key=lambda item: item["name"].casefold())

    def get_series(self, library_ids: Iterable[str]) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for library_id in library_ids:
            page = 0
            while True:
                payload = self._get(
                    f"/api/libraries/{quote(str(library_id), safe='')}/series",
                    params={"limit": 100, "page": page},
                )
                results = payload.get("results", []) if isinstance(payload, dict) else []
                if not isinstance(results, list):
                    raise AudiobookshelfError("Audiobookshelf returned an invalid series response")
                for series in results:
                    if not isinstance(series, dict):
                        continue
                    title = str(series.get("name") or "").strip()
                    key = normalize_series_title(title)
                    if not key:
                        continue
                    entry = merged.setdefault(key, {"title": title, "library_ids": []})
                    if str(library_id) not in entry["library_ids"]:
                        entry["library_ids"].append(str(library_id))
                total = payload.get("total", len(results)) if isinstance(payload, dict) else len(results)
                if not results or (page + 1) * 100 >= int(total or 0):
                    break
                page += 1
        return sorted(merged.values(), key=lambda item: item["title"].casefold())


_cache_lock = threading.Lock()
_series_cache: dict[tuple[str, str, tuple[str, ...]], tuple[float, list[dict[str, Any]]]] = {}


def clear_series_cache() -> None:
    with _cache_lock:
        _series_cache.clear()


def get_cached_series(host: str, api_token: str, library_ids: Iterable[str]) -> list[dict[str, Any]]:
    normalized_host = normalize_host(host)
    ids = tuple(sorted({str(value) for value in library_ids if str(value)}))
    key = (normalized_host, hashlib.sha256(api_token.encode("utf-8")).hexdigest(), ids)
    now = time.monotonic()
    with _cache_lock:
        cached = _series_cache.get(key)
        if cached and now - cached[0] < SERIES_CACHE_TTL_SECONDS:
            return [dict(item) for item in cached[1]]
    series = AudiobookshelfClient(normalized_host, api_token).get_series(ids)
    with _cache_lock:
        _series_cache[key] = (now, series)
    return [dict(item) for item in series]
