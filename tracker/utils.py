import requests
from functools import lru_cache


@lru_cache(maxsize=1024)
def resolve_audible_url(url: str) -> str:
    """Return the final URL after following redirects for Audible links. If not an Audible URL
    or any error occurs, return the original URL unchanged."""
    if not url:
        return url
    try:
        if "audible" not in url.lower():
            return url
        timeout = 3
        # Try HEAD first to avoid fetching body
        try:
            r = requests.head(url, allow_redirects=True, timeout=timeout)
            final = getattr(r, "url", None)
            if final:
                return final
        except Exception:
            pass
        # Fallback to GET if HEAD didn't return a final URL
        try:
            r = requests.get(url, allow_redirects=True, timeout=timeout)
            return getattr(r, "url", url)
        except Exception:
            return url
    except Exception:
        return url