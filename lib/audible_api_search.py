#!/usr/bin/env python3
"""
Tool: audible_api_search.py

Search Audible's external API for audiobooks by title/keywords and return
all available information. The script uses the external API documented at
https://audible.readthedocs.io/en/latest/misc/external_api.html

Usage:
  python tools/audible_api_search.py "The Hobbit" --num-results 5 --raw

Notes:
- Some API endpoints require authentication; if you get a 401 error try
  providing an access token with --auth-token.
- The script defaults to using the public Audible API endpoint at
  https://api.audible.com/1.0/catalog/products
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Dict, Any, Optional
import time
import asyncio
import functools

import requests
from requests.adapters import HTTPAdapter
from cachetools import TTLCache
import shlex
import threading
from prometheus_client import Counter

# ANSI color codes (simple, no external deps)
RESET = "\x1b[0m"
BOLD = "\x1b[1m"
CYAN = "\x1b[36m"
YELLOW = "\x1b[33m"
GREEN = "\x1b[32m"
MAGENTA = "\x1b[35m"
BLUE = "\x1b[34m"

BASE_URL = "https://api.audible.com/1.0/catalog/products"
DEFAULT_RESPONSE_GROUPS = (
    "contributors,media,price,product_attrs,product_desc,product_details,"
    "product_extended_attrs,product_plan_details,product_plans,rating,"
    "sample,sku,series,reviews,relationships,review_attrs,categories"
)

# Simple global rate limiter (default 2 requests/sec => 0.5s interval)
_rate_lock = threading.Lock()
_last_request_time = 0.0
_min_interval = 0.5

# Prometheus counter for Audible API calls
audible_api_calls = Counter('audible_api_calls_total', 'Total number of calls to Audible API')

# In-memory cache for API responses (bounded TTL LRU cache)
CACHE_TTL = 3600  # 1 hour
_cache_lock = threading.Lock()
# Bounded TTL cache to prevent unbounded memory growth
_cache: TTLCache = TTLCache(maxsize=2000, ttl=CACHE_TTL)

# Shared requests.Session for connection pooling and keep-alive
_SESSION = requests.Session()
_SESSION.mount("https://", HTTPAdapter(pool_connections=10, pool_maxsize=20))
_SESSION.mount("http://", HTTPAdapter(pool_connections=10, pool_maxsize=20))

# Background asyncio event loop used for running coroutines from synchronous code
_BG_LOOP: Optional[asyncio.AbstractEventLoop] = None
_BG_THREAD: Optional[threading.Thread] = None


def _start_background_loop() -> None:
    """Start a dedicated background asyncio event loop in a thread."""
    global _BG_LOOP, _BG_THREAD
    if _BG_LOOP and _BG_THREAD and _BG_THREAD.is_alive():
        return
    loop = asyncio.new_event_loop()

    def _run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    t = threading.Thread(target=_run_loop, daemon=True)
    t.start()
    _BG_LOOP = loop
    _BG_THREAD = t


def run_coro_sync(coro: asyncio.coroutines, timeout: float | None = None):
    """Run an awaitable on the background event loop and return its result synchronously.

    If the background loop is not started, this function starts it.
    """
    _start_background_loop()
    if _BG_LOOP is None:
        # Fallback: run directly (shouldn't normally happen)
        return asyncio.run(coro)
    future = asyncio.run_coroutine_threadsafe(coro, _BG_LOOP)
    return future.result(timeout=timeout)


def _get_cache_key(func_name: str, **kwargs) -> str:
    # Create a cache key from function name and relevant kwargs
    key_parts = [func_name]
    for k in sorted(kwargs.keys()):
        if k not in ['proxies', 'user_agent']:  # Ignore non-essential params
            key_parts.append(f"{k}={kwargs[k]}")
    return "|".join(key_parts)


def _get_cached_response(cache_key: str) -> Optional[Dict[str, Any]]:
    with _cache_lock:
        try:
            return _cache[cache_key]
        except KeyError:
            return None


def _set_cached_response(cache_key: str, data: Dict[str, Any]) -> None:
    with _cache_lock:
        try:
            _cache[cache_key] = data
        except Exception:
            # If cache is full or an unexpected error occurs, skip caching
            pass


def set_rate(rps: float) -> None:
    """Configure requests-per-second. rps <= 0 disables sleeping."""
    global _min_interval
    try:
        rps_val = float(rps)
    except Exception:
        return
    if rps_val <= 0:
        _min_interval = 0.0
    else:
        _min_interval = 1.0 / rps_val


async def api_get(url: str, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, str]] = None, timeout: int = 60, proxies: Optional[Dict[str, str]] = None):
    """Rate-limited requests.get wrapper."""
    global _last_request_time
    with _rate_lock:
        now = time.monotonic()
        wait = _min_interval - (now - _last_request_time)
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            # Use shared session to improve connection reuse
            resp = await asyncio.to_thread(_SESSION.get, url, headers=headers, params=params, timeout=timeout, proxies=proxies)
            _last_request_time = time.monotonic()
            # Increment counter for Audible API calls
            if url.startswith(BASE_URL):
                audible_api_calls.inc()
            return resp
        except Exception as e:
            logging.error(f"HTTP request failed: url={url}, error={str(e)}")
            raise


def configure_logger(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(message)s")


def build_query_params(
    title: str,
    num_results: int = 10,
    page: int = 1,
    response_groups: Optional[str] = None,
    marketplace: Optional[str] = None,
) -> Dict[str, str]:
    params: Dict[str, str] = {
        "keywords": title,
        "num_results": str(num_results)
    }
    if response_groups:
        params["response_groups"] = response_groups
    if marketplace:
        params["marketplace"] = marketplace
    return params


async def search_audible(
    title: str,
    num_results: int = 10,
    page: int = 1,
    response_groups: Optional[str] = None,
    auth_token: Optional[str] = None,
    marketplace: Optional[str] = None,
    proxies: Optional[Dict[str, str]] = None,
    user_agent: Optional[str] = None,
) -> Dict[str, Any]:
    """Call the Audible external API for the catalog products endpoint.
    Returns the parsed JSON response (or raises on HTTP error).
    """
    # Check cache first
    cache_key = _get_cache_key('search_audible', title=title, num_results=num_results, page=page, response_groups=response_groups, auth_token=auth_token, marketplace=marketplace)
    cached = _get_cached_response(cache_key)
    if cached:
        return cached

    params = build_query_params(title, num_results, page, response_groups, marketplace)
    headers = {
        "User-Agent": user_agent or "audible-api-search-script/1.0",
        "Accept": "application/json",
    }
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    logging.debug("Query URL: %s", BASE_URL)
    logging.debug("Query params: %s", params)
    logging.debug("Headers: %s", headers)

    # Perform request (rate-limited)
    resp = await api_get(BASE_URL, headers=headers, params=params, timeout=30, proxies=proxies)

    # If debug logging is enabled, show the exact prepared request and a curl equivalent
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        try:
            preq = resp.request  # PreparedRequest used for the actual call
            logging.debug("Prepared request method: %s", preq.method)
            logging.debug("Prepared request URL: %s", preq.url)
            logging.debug("Prepared request headers: %s", dict(preq.headers))
            logging.debug("Prepared request body: %s", preq.body)

            # build a curl representation
            curl_parts = ["curl", "-v", "-X", preq.method]
            for hk, hv in preq.headers.items():
                curl_parts += ["-H", f"{hk}: {hv}"]
            if preq.body:
                body = preq.body
                if isinstance(body, bytes):
                    try:
                        body = body.decode("utf-8")
                    except Exception:
                        body = repr(body)
                curl_parts += ["--data-binary", shlex.quote(str(body))]
            curl_parts.append(shlex.quote(preq.url))
            logging.debug("Equivalent cURL command: %s", " ".join(curl_parts))
        except Exception:
            logging.exception("Failed to log prepared request details")
        try:
            # Log response headers and a truncated body (or JSON snippet) for debugging
            logging.debug("Response headers: %s", dict(resp.headers))
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                try:
                    j = resp.json()
                    snippet = json.dumps(j, indent=2, ensure_ascii=False)
                    logging.debug("Response JSON snippet (truncated 10000 chars): %s", snippet[:10000])
                except Exception:
                    logging.debug("Response text (truncated 10000 chars): %s", resp.text[:10000])
            else:
                logging.debug("Response text (truncated 10000 chars): %s", resp.text[:10000])
        except Exception:
            logging.exception("Failed to log response details")
    logging.debug("Status code: %s", resp.status_code)
    if resp.status_code == 401:
        raise RuntimeError(
            "Unauthorized (401). Some Audible endpoints require authentication. "
            "Try using --auth-token or use other authentication methods."
        )
    resp.raise_for_status()
    result = resp.json()
    # Cache the result
    _set_cached_response(cache_key, result)
    return result


async def get_product_by_asin(
    asin: str,
    response_groups: Optional[str] = None,
    auth_token: Optional[str] = None,
    marketplace: Optional[str] = None,
    proxies: Optional[Dict[str, str]] = None,
    user_agent: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch a single product by ASIN using the products/{asin} endpoint."""
    # Check cache first
    cache_key = _get_cache_key('get_product_by_asin', asin=asin, response_groups=response_groups, auth_token=auth_token, marketplace=marketplace)
    cached = _get_cached_response(cache_key)
    if cached:
        return cached

    url = f"{BASE_URL}/{asin}"
    params: Dict[str, str] = {}
    if response_groups:
        params["response_groups"] = response_groups
    if marketplace:
        params["marketplace"] = marketplace
    headers = {
        "User-Agent": user_agent or "audible-api-search-script/1.0",
        "Accept": "application/json",
    }
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    logging.debug("GET product URL: %s", url)
    logging.debug("Query params: %s", params)
    logging.debug("Headers: %s", headers)

    resp = await api_get(url, headers=headers, params=params, timeout=60, proxies=proxies)

    if logging.getLogger().isEnabledFor(logging.DEBUG):
        try:
            preq = resp.request
            logging.debug("Prepared request URL: %s", preq.url)
            logging.debug("Prepared request headers: %s", dict(preq.headers))
            logging.debug("Prepared request body: %s", preq.body)
            curl_parts = ["curl", "-v", "-X", preq.method]
            for hk, hv in preq.headers.items():
                curl_parts += ["-H", f"{hk}: {hv}"]
            if preq.body:
                body = preq.body
                if isinstance(body, bytes):
                    try:
                        body = body.decode("utf-8")
                    except Exception:
                        body = repr(body)
                curl_parts += ["--data-binary", shlex.quote(str(body))]
            curl_parts.append(shlex.quote(preq.url))
            logging.debug("Equivalent cURL command: %s", " ".join(curl_parts))
        except Exception:
            logging.exception("Failed to log prepared product request details")
        try:
            logging.debug("Response headers: %s", dict(resp.headers))
            ct = resp.headers.get("Content-Type", "")
            if "application/json" in ct:
                try:
                    j = resp.json()
                    logging.debug("Response JSON snippet (truncated): %s", json.dumps(j, indent=2, ensure_ascii=False)[:10000])
                except Exception:
                    logging.debug("Response text (truncated): %s", resp.text[:10000])
            else:
                logging.debug("Response text (truncated): %s", resp.text[:10000])
        except Exception:
            logging.exception("Failed to log product response details")

    resp.raise_for_status()
    result = resp.json()
    # Cache the result
    _set_cached_response(cache_key, result)
    return result





def pretty_print_product(product: Dict[str, Any], color: bool = True) -> None:
    """Prints a nicely formatted set of fields from a product entry.

    `color` toggles ANSI-colored labels/values.
    """
    title = product.get("title") or product.get("product_title") or "Unknown Title"
    asin = product.get("asin") or product.get("asin2") or "Unknown ASIN"
    authors = [a.get("name") for a in product.get("contributors", [])]
    narrators = [a.get("name") for a in product.get("narrators", [])]
    series = [a.get("title") for a in product.get("series", [])]
    release_date = product.get("release_date") or product.get("release_date")
    runtime = product.get("runtime_length_min")
    product_images = product.get("product_images", {})
    image = next(iter(product_images.values()), None) if product_images else None

    def L(label: str) -> str:
        return _color_text(label, BOLD, color)

    def V(value: str, col: str = CYAN) -> str:
        return _color_text(value, col, color)

    print(f"{L('Title:')} {V(title, CYAN)}")
    print(f"{L('ASIN:')} {V(asin, YELLOW)}")
    if series:
        print(f"{L('Series:')} {V(', '.join(series), MAGENTA)}")
    if authors:
        print(f"{L('Author(s):')} {V(', '.join(authors), MAGENTA)}")
    if narrators:
        print(f"{L('Narrator(s):')} {V(', '.join(narrators), MAGENTA)}")
    if release_date:
        print(f"{L('Release Date:')} {V(str(release_date), GREEN)}")
    if runtime:
        print(f"{L('Runtime:')} {V(str(runtime), GREEN)}")
    if image:
        print(f"{L('Image:')} {V(str(image), GREEN)}")
    # if price:
    #     print(f"{L('Price:')} {V(str(price), YELLOW)}")
    # if product.get("rating"):
    #     print(f"{L('Avg Rating:')} {V(str(product.get('rating')), BLUE)}")
    # if product.get("num_reviews"):
    #     print(f"{L('Number of Reviews:')} {V(str(product.get('num_reviews')), BLUE)}")
    product_url = product.get("url") or product.get("product_url")
    if product_url:
        print(f"{L('URL:')} {V(product_url, BLUE)}")

    # JSON snippet (uncolored for readability)
    # print(_color_text("JSON snippet:", BOLD, color))
    # print(json.dumps(product, indent=2, ensure_ascii=False))
    print("---\n")


def _color_text(text: str, color_code: str, enabled: bool) -> str:
    if not enabled:
        return text
    return f"{color_code}{text}{RESET}"


def _format_series_url(url: str) -> str:
    """Normalize series URL: replace /pd/ with /series/ and add domain prefix when needed."""
    if not url:
        return url
    # If already absolute, leave as-is
    if url.startswith("http://") or url.startswith("https://"):
        return url
    # Replace /pd/ with /series/ for Audible product->series mapping
    if url.startswith("/pd/"):
        url = url.replace("/pd/", "/series/", 1)
    # Ensure leading slash
    if not url.startswith("/"):
        url = "/" + url
    return "https://www.audible.com" + url


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(description="Search Audible using the external API")
    parser.add_argument("query", nargs="?", help="Search string (title)")
    parser.add_argument("--num-results", type=int, default=10, help="Number of results to return (max 50)")
    parser.add_argument("--page", type=int, default=1, help="Page number")
    parser.add_argument("--response-groups", default=DEFAULT_RESPONSE_GROUPS, help="Comma-separated response_groups to request")
    parser.add_argument("--marketplace", default=None, help="Market place (e.g. AN7V1F1VY261K)")
    parser.add_argument("--auth-token", default=None, help="Bearer token for Authorization if required")
    parser.add_argument("--raw", action="store_true", help="Print raw JSON response instead of pretty output")
    parser.add_argument("--output", default=None, help="Save JSON response to file")
    parser.add_argument("--series", action="store_true", help="Treat query as a series name and filter results to items in that series")
    parser.add_argument("--series-books", metavar="ASIN", help="Given a series ASIN (or a book ASIN), list books in that series")
    # Color flag: allow --color or --no-color; default auto-detects TTY
    color_group = parser.add_mutually_exclusive_group()
    color_group.add_argument("--color", dest="color", action="store_true", help="Enable colored pretty output")
    color_group.add_argument("--no-color", dest="color", action="store_false", help="Disable colored output")
    parser.set_defaults(color=None)
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--rps", type=float, default=2.0, help="Requests per second for rate limiting (default 2.0)")

    args = parser.parse_args(argv)

    configure_logger(args.debug)
    set_rate(args.rps)

    # Determine whether to use color: CLI overrides; otherwise auto-detect TTY
    if args.color is None:
        color_enabled = sys.stdout.isatty()
    else:
        color_enabled = bool(args.color)

    # If --series-books is provided, fetch the series (or parent series) by ASIN
    if args.series_books:
        try:
            prod = get_product_by_asin(args.series_books, response_groups=args.response_groups, auth_token=args.auth_token, marketplace=args.marketplace)
        except Exception as e:
            logging.error(f"Error fetching product {args.series_books}: {e}")
            sys.exit(2)
        # normalize product wrapper: some endpoints return {'product': {...}}
        prod_obj = prod.get("product") if isinstance(prod, dict) and "product" in prod else prod

        # If the provided ASIN is a book, find a parent series and load that instead
        series_asin = None
        for rel in prod_obj.get("relationships", []):
            if isinstance(rel, dict) and (rel.get("relationship_type") == "series" and rel.get("relationship_to_product") == "parent"):
                series_asin = rel.get("asin")
                break
        if not series_asin:
            # if the product itself appears to be a series (has content_delivery_type BookSeries or relationships indicating children), treat as series
            if prod_obj.get("content_delivery_type") == "BookSeries" or any(isinstance(r, dict) and r.get("relationship_to_product") == "child" for r in prod_obj.get("relationships", [])):
                series_asin = args.series_books

        if not series_asin:
            logging.error("Provided ASIN does not appear to be part of a series and no parent series was found.")
            sys.exit(2)

        # Fetch the series product details
        try:
            series_prod = get_product_by_asin(series_asin, response_groups=args.response_groups, auth_token=args.auth_token, marketplace=args.marketplace)
        except Exception as e:
            logging.error(f"Error fetching series product {series_asin}: {e}")
            sys.exit(2)

        series_obj = series_prod.get("product") if isinstance(series_prod, dict) and "product" in series_prod else series_prod

        # collect child ASINs from relationships where relationship_to_product indicates child/component
        child_entries = []
        for rel in series_obj.get("relationships", []):
            if not isinstance(rel, dict):
                continue
            if rel.get("relationship_to_product") in ("child",) or rel.get("relationship_type") in ("component", "series"):
                child_entries.append(rel)

        if not child_entries:
            print(_color_text("No child books found for this series.", YELLOW, color_enabled))
            return

        # try to preserve sequence if present
        def _rel_sort_key(r: Dict[str, Any]):
            seq = r.get("sequence") or r.get("sort")
            try:
                return int(seq)
            except Exception:
                return 0

        child_entries.sort(key=_rel_sort_key)
        
        for idx, rel in enumerate(child_entries, start=1):
            child_asin = rel.get("asin")
            if not child_asin:
                continue
            try:
                child_prod = get_product_by_asin(child_asin, response_groups=args.response_groups, auth_token=args.auth_token, marketplace=args.marketplace).get("product")
            except Exception:
                # fallback: print asin only
                print(f"{idx}. ASIN: {child_asin}")
                continue
            # print concise info per book
            # print(child_prod)
            # print(json.dumps(child_prod, indent=2, ensure_ascii=False))
            title = child_prod.get("title") or child_prod.get("publication_name") or child_prod.get("product_title") or "Unknown"
            narrators = [n.get("name") for n in child_prod.get("narrators", []) if isinstance(n, dict)]
            narrator_str = ", ".join(narrators) if narrators else ""
            line = f"{idx}. {title} — {child_asin}"
            if narrator_str:
                line += f" — {narrator_str}"
            print(_color_text(line, CYAN, color_enabled))
        return

    try:
        response = search_audible(
            args.query,
            num_results=args.num_results,
            page=args.page,
            response_groups=args.response_groups,
        )
    except Exception as e:
        logging.error(f"Error searching Audible: {e}")
        sys.exit(2)

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as fh:
                json.dump(response, fh, indent=2, ensure_ascii=False)
            print(f"Saved JSON response to {args.output}")
        except Exception as exc:
            logging.error("Failed to write output file: %s", exc)

    if args.raw:
        print(json.dumps(response, indent=2, ensure_ascii=False))
    else:
        # Response usually contains top-level keys; try to find 'products' or 'products' in 'search_results'
        products = None
        if isinstance(response, dict):
            for candidate in ("products", "Items", "items", "search_results", "SearchResults"):
                if candidate in response and isinstance(response[candidate], list):
                    products = response[candidate]
                    break
        if products is None:
            # Fallback: try to pretty print top-level response
            print(json.dumps(response, indent=2, ensure_ascii=False))
            return

        # If user requested series search, perform two-step grouping:
        # 1) search returns audiobook products (by name)
        # 2) extract a stable series key for each product
        # 3) group all discovered series titles by that series key and uniq them
        def _extract_series_key_and_titles(product: Dict[str, Any]) -> tuple:
            """Return (series_key, uniq_titles, uniq_urls, uniq_asins) extracted from product."""
            titles: list = []
            keys: list = []
            urls: list = []
            asins: list = []
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
                if "url" in obj and obj.get("url"):
                    urls.append(obj.get("url"))
                if "asin" in obj and obj.get("asin"):
                    asins.append(str(obj.get("asin")))

            if isinstance(s, dict):
                _collect_from_obj(s)
            elif isinstance(s, list):
                for e in s:
                    if isinstance(e, dict):
                        _collect_from_obj(e)
                    elif isinstance(e, str):
                        titles.append(e)

            # also inspect relationships for series information
            for rel in product.get("relationships", []):
                if not isinstance(rel, dict):
                    continue
                # treat parent/series relationships as series
                if rel.get("relationship_type") == "series" or rel.get("relationship_to_product") == "parent":
                    if rel.get("asin"):
                        keys.append(str(rel.get("asin")))
                    if rel.get("title"):
                        titles.append(rel.get("title"))
                    if rel.get("url"):
                        urls.append(rel.get("url"))
                    if rel.get("asin"):
                        asins.append(str(rel.get("asin")))

            # also check some common top-level keys
            for key in ("series_id", "product_series_id", "seriesAsin", "series_asin"):
                if key in product and product.get(key):
                    keys.append(str(product.get(key)))
            for key in ("series_title", "product_series_title"):
                if key in product and isinstance(product[key], str):
                    titles.append(product[key])
            if "url" in product and product.get("url"):
                urls.append(product.get("url"))
            if "asin" in product and product.get("asin"):
                asins.append(str(product.get("asin")))

            # dedupe preserving order
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
            return key, uniq_titles, uniq_urls, uniq_asins

        if args.series:
            # group by series key, tracking total occurrences (non-unique count)
            groups: Dict[str, Dict[str, object]] = {}
            for p in products:
                key, titles, urls, asins = _extract_series_key_and_titles(p)
                group_key = key or "__no_series_key__"
                entry = groups.setdefault(group_key, {"count": 0, "titles": {}})
                entry["count"] = int(entry.get("count", 0)) + 1
                for t in titles:
                    entry["titles"].setdefault(t, {"urls": set(), "asins": set()})
                    entry["titles"][t]["urls"].update(urls)
                    entry["titles"][t]["asins"].update(asins)
            if not groups:
                print(_color_text(f"No series discovered in results for '{args.query}'", YELLOW, color_enabled))
            else:
                # sort groups by non-unique occurrence count (descending)
                sorted_groups = sorted(groups.items(), key=lambda kv: kv[1]["count"], reverse=True)
                for k, entry in sorted_groups:
                    cnt = entry.get("count", 0)
                    title_map = entry.get("titles", {})
                    # print titles and their associated urls (if any)
                    for t in sorted(title_map.keys()):
                        data = title_map.get(t, {})
                        urls_for_t = sorted(data.get("urls", []))
                        asins_for_t = sorted(data.get("asins", []))
                        parts = []
                        if urls_for_t:
                            parts.append(", ".join(_format_series_url(u) for u in urls_for_t))
                        # only include the first ASIN (if any)
                        if asins_for_t:
                            parts.append("ASIN: " + asins_for_t[0])
                        if parts:
                            print("  " + _color_text(t, CYAN, color_enabled) + " -> " + ", ".join(_color_text(p, BLUE, color_enabled) for p in parts))
                        else:
                            print("  " + _color_text(t, CYAN, color_enabled))
                    print()
        else:
            for product in products:
                pretty_print_product(product, color=color_enabled)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Gracefully handle Ctrl-C without a traceback
        sys.stderr.write("Interrupted.\n")
        sys.exit(130)
