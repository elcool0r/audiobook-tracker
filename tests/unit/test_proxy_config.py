"""`requests` proxy mapping format.

_build_proxies previously returned httpx-style keys ("http://", "https://"),
which `requests` never matches — so every outbound call went direct while the
settings UI reported the proxy as working.
"""
import requests

from tracker.library import _build_proxies
from tracker.settings import default_settings


def _settings(**overrides):
    return default_settings().model_copy(update=overrides)


def test_returns_bare_scheme_keys():
    proxies = _build_proxies(_settings(proxy_enabled=True, proxy_url="http://proxy:8080"))
    assert set(proxies) == {"http", "https"}


def test_keys_are_understood_by_requests():
    """The authoritative check: requests must actually select the proxy."""
    proxies = _build_proxies(_settings(proxy_enabled=True, proxy_url="http://proxy:8080"))
    session = requests.Session()
    resolved = session.rebuild_proxies(
        requests.Request("GET", "https://api.audible.com/1.0/catalog/products").prepare(),
        proxies,
    )
    assert resolved.get("https") == "http://proxy:8080"


def test_credentials_are_injected_into_the_proxy_url():
    proxies = _build_proxies(_settings(
        proxy_enabled=True, proxy_url="socks5://proxy:1080",
        proxy_username="bob", proxy_password="hunter2",
    ))
    assert proxies["https"] == "socks5://bob:hunter2@proxy:1080"


def test_disabled_proxy_returns_nothing():
    assert _build_proxies(_settings(proxy_enabled=False, proxy_url="http://proxy:8080")) is None
    assert _build_proxies(_settings(proxy_enabled=True, proxy_url=None)) is None
