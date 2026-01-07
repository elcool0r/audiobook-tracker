import importlib
import os
from pathlib import Path

import tracker


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_get_version_env_non_special(monkeypatch):
    monkeypatch.setenv('VERSION', 'v2.3.4')
    import tracker.__version__ as version_mod
    importlib.reload(version_mod)
    assert version_mod.get_version() == '2.3.4'


def test_get_version_env_dev_and_version_file(monkeypatch, tmp_path):
    project_root = _project_root()
    version_file = project_root / 'VERSION'
    # Backup existing
    backup = None
    if version_file.exists():
        backup = version_file.read_text()
    try:
        version_file.write_text('v2.3.4')
        monkeypatch.setenv('VERSION', 'dev')
        import tracker.__version__ as version_mod
        importlib.reload(version_mod)
        assert version_mod.get_version() == '2.3.4-dev'
    finally:
        if backup is None:
            version_file.unlink(missing_ok=True)
        else:
            version_file.write_text(backup)


def test_navbar_shows_dev_suffix(monkeypatch, tmp_path):
    # Create VERSION file and set env
    project_root = _project_root()
    version_file = project_root / 'VERSION'
    backup = None
    if version_file.exists():
        backup = version_file.read_text()
    try:
        version_file.write_text('v2.3.4')
        monkeypatch.setenv('VERSION', 'dev')
        # Reload version module then reload app module so it picks up new __version__
        import tracker.__version__ as version_mod
        importlib.reload(version_mod)
        import tracker.app as app_mod
        importlib.reload(app_mod)
        from fastapi.testclient import TestClient
        client = TestClient(app_mod.create_app())
        resp = client.get('/')
        assert resp.status_code == 200
        assert '2.3.4-dev' in resp.text
    finally:
        if backup is None:
            version_file.unlink(missing_ok=True)
        else:
            version_file.write_text(backup)
