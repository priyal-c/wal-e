"""Tests for WAL-E configuration and auth validation."""

import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

from wal_e.core.config import WalEConfig, detect_cloud_provider


def test_detect_cloud_provider():
    """Cloud detection from workspace URL."""
    assert detect_cloud_provider("https://acme.cloud.databricks.com") == "aws"
    assert detect_cloud_provider("https://acme.azuredatabricks.net") == "azure"
    assert detect_cloud_provider("https://acme.gcp.databricks.com") == "gcp"
    assert detect_cloud_provider("") == "unknown"
    assert detect_cloud_provider("https://random.example.com") == "unknown"


def test_config_loads_pat_profile():
    """Config loads host and token from a PAT-based profile."""
    cfg_content = "[test-pat]\nhost = https://acme.cloud.databricks.com\ntoken = dapi123\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False) as f:
        f.write(cfg_content)
        f.flush()
        with patch.dict("os.environ", {"DATABRICKS_CONFIG_FILE": f.name}):
            config = WalEConfig(profile_name="test-pat")
            assert config.workspace_host == "https://acme.cloud.databricks.com"
            assert config.token == "dapi123"
            assert config.cloud_provider == "aws"


def test_config_loads_oauth_profile():
    """Config loads host from an OAuth profile (no token field)."""
    cfg_content = "[test-oauth]\nhost = https://acme.cloud.databricks.com\nauth_type = databricks-cli\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False) as f:
        f.write(cfg_content)
        f.flush()
        with patch.dict("os.environ", {"DATABRICKS_CONFIG_FILE": f.name}):
            config = WalEConfig(profile_name="test-oauth")
            assert config.workspace_host == "https://acme.cloud.databricks.com"
            assert config.token == ""  # no token in OAuth profiles
            assert config.cloud_provider == "aws"


def test_validate_no_host():
    """Validation fails when no workspace host is configured."""
    cfg_content = "[empty]\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False) as f:
        f.write(cfg_content)
        f.flush()
        with patch.dict("os.environ", {"DATABRICKS_CONFIG_FILE": f.name}):
            config = WalEConfig(profile_name="empty")
            ok, msg = config.validate()
            assert not ok
            assert "host not configured" in msg.lower()


def _mock_run_factory(auth_ok=True, api_ok=True):
    """Create a mock subprocess.run that simulates auth describe + API call."""
    call_count = 0

    def mock_run(cmd, **kwargs):
        nonlocal call_count
        call_count += 1
        result = subprocess.CompletedProcess(cmd, returncode=0, stdout="{}", stderr="")
        if "auth" in cmd and "describe" in cmd:
            if not auth_ok:
                result.returncode = 1
                result.stderr = "no profile configured"
        elif "api" in cmd:
            if not api_ok:
                result.returncode = 1
                result.stderr = "401 Unauthorized"
        return result

    return mock_run


def test_validate_pat_success():
    """Validation succeeds for a PAT profile."""
    cfg_content = "[pat]\nhost = https://acme.cloud.databricks.com\ntoken = dapi123\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False) as f:
        f.write(cfg_content)
        f.flush()
        with patch.dict("os.environ", {"DATABRICKS_CONFIG_FILE": f.name}):
            config = WalEConfig(profile_name="pat")
            with patch("subprocess.run", side_effect=_mock_run_factory(auth_ok=True, api_ok=True)):
                ok, msg = config.validate()
                assert ok
                assert "validated successfully" in msg.lower()


def test_validate_oauth_success():
    """Validation succeeds for an OAuth profile (no token)."""
    cfg_content = "[oauth]\nhost = https://acme.cloud.databricks.com\nauth_type = databricks-cli\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False) as f:
        f.write(cfg_content)
        f.flush()
        with patch.dict("os.environ", {"DATABRICKS_CONFIG_FILE": f.name}):
            config = WalEConfig(profile_name="oauth")
            with patch("subprocess.run", side_effect=_mock_run_factory(auth_ok=True, api_ok=True)):
                ok, msg = config.validate()
                assert ok
                assert "validated successfully" in msg.lower()


def test_validate_auth_failure():
    """Validation fails when auth describe fails (bad or missing profile)."""
    cfg_content = "[bad]\nhost = https://acme.cloud.databricks.com\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False) as f:
        f.write(cfg_content)
        f.flush()
        with patch.dict("os.environ", {"DATABRICKS_CONFIG_FILE": f.name}):
            config = WalEConfig(profile_name="bad")
            with patch("subprocess.run", side_effect=_mock_run_factory(auth_ok=False)):
                ok, msg = config.validate()
                assert not ok
                assert "authentication not configured" in msg.lower()
                assert "oauth" in msg.lower()  # error mentions both auth options


def test_validate_api_failure():
    """Auth passes but API connectivity fails (expired token, network, etc.)."""
    cfg_content = "[expired]\nhost = https://acme.cloud.databricks.com\ntoken = dapi_expired\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False) as f:
        f.write(cfg_content)
        f.flush()
        with patch.dict("os.environ", {"DATABRICKS_CONFIG_FILE": f.name}):
            config = WalEConfig(profile_name="expired")
            with patch("subprocess.run", side_effect=_mock_run_factory(auth_ok=True, api_ok=False)):
                ok, msg = config.validate()
                assert not ok
                assert "connection failed" in msg.lower()


if __name__ == "__main__":
    test_detect_cloud_provider()
    test_config_loads_pat_profile()
    test_config_loads_oauth_profile()
    test_validate_no_host()
    test_validate_pat_success()
    test_validate_oauth_success()
    test_validate_auth_failure()
    test_validate_api_failure()
    print("All config tests passed.")
