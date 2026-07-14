from unittest.mock import Mock

import pytest
import requests

from tracker.audiobookshelf import (
    AudiobookshelfClient,
    AudiobookshelfError,
    clear_series_cache,
    get_cached_series,
    normalize_host,
    normalize_series_title,
)


def response(payload, status=200, redirect=False):
    result = Mock()
    result.ok = 200 <= status < 300
    result.status_code = status
    result.is_redirect = redirect
    result.json.return_value = payload
    return result


@pytest.mark.parametrize("host", ["", "abs.local", "ftp://abs.local", "https://user:pass@abs.local"])
def test_invalid_hosts_are_rejected(host):
    with pytest.raises(AudiobookshelfError):
        normalize_host(host)


def test_host_and_title_normalization():
    assert normalize_host("https://abs.local/base/") == "https://abs.local/base"
    assert normalize_series_title("  The Expanse: Series ") == "the expanse"


def test_libraries_use_bearer_token_and_only_return_books(monkeypatch):
    get = Mock(return_value=response({"libraries": [
        {"id": "podcasts", "name": "Podcasts", "mediaType": "podcast"},
        {"id": "books", "name": "Audiobooks", "mediaType": "book"},
    ]}))
    monkeypatch.setattr("tracker.audiobookshelf.requests.get", get)

    libraries = AudiobookshelfClient("http://abs.local/", "secret").get_libraries()

    assert libraries == [{"id": "books", "name": "Audiobooks"}]
    assert get.call_args.kwargs["headers"] == {"Authorization": "Bearer secret"}
    assert get.call_args.kwargs["timeout"] == 10
    assert get.call_args.kwargs["allow_redirects"] is False


def test_series_are_paginated_and_merged_across_libraries(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs["params"]))
        library_id = url.split("/")[-2]
        page = kwargs["params"]["page"]
        if library_id == "one" and page == 0:
            return response({"results": [{"name": "Shared Series"}] * 100, "total": 101})
        if library_id == "one":
            return response({"results": [{"name": "Only One"}], "total": 101})
        return response({"results": [{"name": "Shared"}], "total": 1})

    monkeypatch.setattr("tracker.audiobookshelf.requests.get", fake_get)
    series = AudiobookshelfClient("http://abs.local", "secret").get_series(["one", "two"])

    assert [item["title"] for item in series] == ["Only One", "Shared Series"]
    shared = next(item for item in series if item["title"] == "Shared Series")
    assert shared["library_ids"] == ["one", "two"]
    assert len(calls) == 3


def test_redirects_auth_errors_and_network_errors_are_safe(monkeypatch):
    monkeypatch.setattr("tracker.audiobookshelf.requests.get", Mock(return_value=response({}, 302, True)))
    with pytest.raises(AudiobookshelfError, match="redirect"):
        AudiobookshelfClient("http://abs.local", "secret").get_libraries()

    monkeypatch.setattr("tracker.audiobookshelf.requests.get", Mock(return_value=response({}, 401)))
    with pytest.raises(AudiobookshelfError, match="rejected"):
        AudiobookshelfClient("http://abs.local", "secret").get_libraries()

    monkeypatch.setattr("tracker.audiobookshelf.requests.get", Mock(side_effect=requests.Timeout("timed out")))
    with pytest.raises(AudiobookshelfError, match="Could not connect"):
        AudiobookshelfClient("http://abs.local", "secret").get_libraries()


def test_series_cache_reuses_results(monkeypatch):
    clear_series_cache()
    get_series = Mock(return_value=[{"title": "Cached", "library_ids": ["one"]}])
    monkeypatch.setattr("tracker.audiobookshelf.AudiobookshelfClient.get_series", get_series)

    first = get_cached_series("http://abs.local", "secret", ["one"])
    second = get_cached_series("http://abs.local", "secret", ["one"])

    assert first == second
    get_series.assert_called_once()
