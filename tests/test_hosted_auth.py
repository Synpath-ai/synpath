"""Hosted CLI credential storage is private and usable by the history client."""
import os

import pytest

from synpath.hosted import api_key_from
from synpath.hosted_auth import credentials_path, load_credentials, save_credentials


def test_saved_key_is_private_and_available_to_hosted_client(tmp_path, monkeypatch):
    path = tmp_path / "private" / "credentials.json"
    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(path))
    monkeypatch.delenv("SYNPATH_API_KEY", raising=False)
    save_credentials({"api_key": "spk_test.value", "session_token": "session"})
    assert credentials_path() == path
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert load_credentials()["api_key"] == "spk_test.value"
    assert api_key_from(use_stored=True) == "spk_test.value"
    assert api_key_from() is None
    monkeypatch.setenv("SYNPATH_API_KEY", "environment-key")
    assert api_key_from() == "environment-key"


@pytest.mark.skipif(os.name == "nt", reason="POSIX permissions")
def test_rejects_world_readable_credentials(tmp_path, monkeypatch):
    path = tmp_path / "credentials.json"
    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(path))
    save_credentials({"api_key": "secret"})
    path.chmod(0o644)
    with pytest.raises(RuntimeError, match="chmod 600"):
        load_credentials()
