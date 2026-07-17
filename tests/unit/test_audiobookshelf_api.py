from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from tracker.api import (
    AudiobookshelfTestRequest,
    SettingsSaveRequest,
    api_get_settings,
    api_list_audiobookshelf_series,
    api_remove_audiobookshelf,
    api_save_settings,
    api_test_audiobookshelf,
)
from tracker.audiobookshelf import AudiobookshelfError
from tracker.settings import default_settings


ADMIN = {"username": "admin", "role": "admin"}


@pytest.mark.asyncio
async def test_connection_test_saves_verified_config_and_preserves_failed_config():
    current = default_settings()
    client = Mock(host="http://abs.local")
    client.get_libraries.return_value = [{"id": "one", "name": "Audiobooks"}]
    with patch("tracker.api.load_settings", return_value=current), \
         patch("tracker.api.AudiobookshelfClient", return_value=client), \
         patch("tracker.api.save_settings") as save:
        result = await api_test_audiobookshelf(
            AudiobookshelfTestRequest(host="http://abs.local", api_token="secret"),
            user=ADMIN,
        )
    assert result["success"] is True
    saved = save.call_args.args[0]
    assert saved.audiobookshelf_api_token == "secret"
    assert saved.audiobookshelf_libraries == [{"id": "one", "name": "Audiobooks"}]

    with patch("tracker.api.load_settings", return_value=saved), \
         patch("tracker.api.AudiobookshelfClient", side_effect=AudiobookshelfError("offline")), \
         patch("tracker.api.save_settings") as failed_save:
        with pytest.raises(HTTPException) as exc:
            await api_test_audiobookshelf(
                AudiobookshelfTestRequest(host="http://other.local", api_token="bad"),
                user=ADMIN,
            )
        assert exc.value.detail == "offline"
    failed_save.assert_not_called()


@pytest.mark.asyncio
async def test_settings_response_masks_token_and_library_selection_is_validated():
    current = default_settings().model_copy(update={
        "audiobookshelf_api_token": "secret",
        "audiobookshelf_libraries": [{"id": "one", "name": "Audiobooks"}],
    })
    with patch("tracker.api.load_settings", return_value=current):
        result = await api_get_settings(user=ADMIN)
    assert "audiobookshelf_api_token" not in result
    assert result["audiobookshelf_api_token_configured"] is True

    with patch("tracker.api.load_settings", return_value=current), patch("tracker.api.save_settings"):
        with pytest.raises(HTTPException) as exc:
            await api_save_settings(SettingsSaveRequest(audiobookshelf_library_ids=["missing"]), user=ADMIN)
        assert exc.value.detail == "Select only libraries returned by the connection test"


@pytest.mark.asyncio
async def test_series_endpoint_filters_known_titles_and_adds_library_names():
    settings = default_settings().model_copy(update={
        "audiobookshelf_host": "http://abs.local",
        "audiobookshelf_api_token": "secret",
        "audiobookshelf_connection_ok": True,
        "audiobookshelf_library_ids": ["one"],
        "audiobookshelf_libraries": [{"id": "one", "name": "Audiobooks"}],
    })
    collection = Mock()
    collection.find.return_value = [{"title": "Known Series", "original_title": "Original Name"}]
    source = [
        {"title": "Known", "library_ids": ["one"]},
        {"title": "Original Name", "library_ids": ["one"]},
        {"title": "New Series", "library_ids": ["one"]},
    ]
    with patch("tracker.api.load_settings", return_value=settings), \
         patch("tracker.api.get_cached_series", return_value=source), \
         patch("tracker.api.get_series_collection", return_value=collection):
        result = await api_list_audiobookshelf_series(user=ADMIN)
    assert result == [{"title": "New Series", "libraries": ["Audiobooks"]}]


@pytest.mark.asyncio
async def test_remove_requires_admin_and_clears_configuration():
    with pytest.raises(HTTPException) as test_exc:
        await api_test_audiobookshelf(
            AudiobookshelfTestRequest(host="http://abs.local", api_token="secret"),
            user={"username": "user", "role": "user"},
        )
    assert test_exc.value.status_code == 403

    with pytest.raises(HTTPException) as exc:
        await api_remove_audiobookshelf(user={"username": "user", "role": "user"})
    assert exc.value.status_code == 403

    current = default_settings().model_copy(update={
        "audiobookshelf_host": "http://abs.local",
        "audiobookshelf_api_token": "secret",
        "audiobookshelf_connection_ok": True,
    })
    with patch("tracker.api.load_settings", return_value=current), patch("tracker.api.save_settings") as save:
        await api_remove_audiobookshelf(user=ADMIN)
    saved = save.call_args.args[0]
    assert saved.audiobookshelf_host is None
    assert saved.audiobookshelf_api_token is None
