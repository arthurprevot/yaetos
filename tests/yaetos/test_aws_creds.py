"""Unit tests for yaetos/aws_creds.py — AWS credentials management.

Only boto3 and yaetos.logger need mocking (the only external deps of aws_creds.py).
"""
import os
import sys
import pytest
from unittest.mock import MagicMock


# ── Mock only what aws_creds.py actually imports ──
_mock_boto3 = MagicMock()
_mock_session = MagicMock()
_mock_creds = MagicMock()
_mock_creds.access_key = "DEFAULT_KEY"
_mock_creds.secret_key = "DEFAULT_SECRET"
_mock_creds.token = ""
_mock_session.get_credentials.return_value = _mock_creds
_mock_boto3.Session.return_value = _mock_session
sys.modules["boto3"] = _mock_boto3

# Mock botocore exceptions with real-ish exception classes
_mock_botocore = MagicMock()

class _NoCredentialsError(Exception):
    pass

class _PartialCredentialsError(Exception):
    def __init__(self, **kwargs):
        super().__init__()

class _ClientError(Exception):
    pass

_mock_botocore.exceptions.NoCredentialsError = _NoCredentialsError
_mock_botocore.exceptions.PartialCredentialsError = _PartialCredentialsError
_mock_botocore.exceptions.ClientError = _ClientError
sys.modules["botocore"] = _mock_botocore
sys.modules["botocore.exceptions"] = _mock_botocore.exceptions

mock_logger = MagicMock()
sys.modules["yaetos.logger"] = MagicMock()
sys.modules["yaetos.logger"].setup_logging = MagicMock(return_value=mock_logger)

from yaetos.aws_creds import (
    get_aws_setup,
    get_session_from_direct_creds,
    get_session_from_profile,
    test_aws_connection as verify_aws_connection,
)


def _reset():
    _mock_boto3.Session.reset_mock()
    _mock_creds.access_key = "DEFAULT_KEY"
    _mock_creds.secret_key = "DEFAULT_SECRET"
    _mock_creds.token = ""


def _write_config(tmp_path, section="dev", **fields):
    """Helper to create a temp aws_config.cfg file."""
    path = os.path.join(str(tmp_path), "aws_config.cfg")
    lines = [f"[{section}]"]
    field_map = {
        "access_key": "aws_access_key_id",
        "secret_key": "aws_secret_access_key",
        "session_token": "aws_session_token",
        "region": "s3_region",
        "profile_name": "profile_name",
    }
    for key, value in fields.items():
        if value is not None:
            lines.append(f"{field_map[key]} : {value}")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


def _clear_env():
    for k in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
        os.environ.pop(k, None)


# ─── get_aws_setup: Direct Credentials (Option 1) ──────


class TestDirectCreds:

    def setup_method(self):
        _reset()
        _clear_env()

    def test_creates_session_with_keys(self, tmp_path):
        cfg = _write_config(tmp_path, access_key="AKIATEST", secret_key="SECRET")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        _mock_boto3.Session.assert_called_once_with(
            aws_access_key_id="AKIATEST",
            aws_secret_access_key="SECRET",
            aws_session_token=None,
            region_name=None,
        )

    def test_passes_session_token(self, tmp_path):
        cfg = _write_config(tmp_path, access_key="AK", secret_key="SK", session_token="TOK")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        assert _mock_boto3.Session.call_args[1]["aws_session_token"] == "TOK"

    def test_passes_region(self, tmp_path):
        cfg = _write_config(tmp_path, access_key="AK", secret_key="SK", region="eu-west-1")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        assert _mock_boto3.Session.call_args[1]["region_name"] == "eu-west-1"

    def test_sets_env_vars(self, tmp_path):
        _mock_creds.access_key = "ENVKEY"
        _mock_creds.secret_key = "ENVSEC"
        _mock_creds.token = "ENVTOK"
        cfg = _write_config(tmp_path, access_key="AK", secret_key="SK")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        assert os.environ["AWS_ACCESS_KEY_ID"] == "ENVKEY"
        assert os.environ["AWS_SECRET_ACCESS_KEY"] == "ENVSEC"
        assert os.environ["AWS_SESSION_TOKEN"] == "ENVTOK"

    def test_strips_whitespace(self, tmp_path):
        cfg = _write_config(tmp_path, access_key="  SPACED_AK  ", secret_key="  SPACED_SK  ")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        kw = _mock_boto3.Session.call_args[1]
        assert kw["aws_access_key_id"] == "SPACED_AK"
        assert kw["aws_secret_access_key"] == "SPACED_SK"


# ─── get_aws_setup: Profile Fallback (Option 2) ────────


class TestProfileFallback:

    def setup_method(self):
        _reset()
        _clear_env()

    def test_uses_profile_when_no_direct_creds(self, tmp_path):
        cfg = _write_config(tmp_path, profile_name="my_profile")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        _mock_boto3.Session.assert_called_once_with(profile_name="my_profile")

    def test_direct_creds_take_precedence(self, tmp_path):
        cfg = _write_config(tmp_path, access_key="AK", secret_key="SK", profile_name="ignored")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        kw = _mock_boto3.Session.call_args[1]
        assert kw["aws_access_key_id"] == "AK"
        assert "profile_name" not in kw

    def test_only_access_key_falls_back_to_profile(self, tmp_path):
        cfg = _write_config(tmp_path, access_key="AKONLY", profile_name="fallback")
        get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})
        _mock_boto3.Session.assert_called_once_with(profile_name="fallback")


# ─── get_session_from_direct_creds ──────────────────


class TestGetSessionFromDirectCreds:

    def setup_method(self):
        _reset()

    def test_raises_without_secret(self, tmp_path):
        from configparser import ConfigParser
        cfg = ConfigParser()
        cfg.read_string("[dev]\naws_access_key_id: AK\n")
        with pytest.raises(ValueError, match="aws_secret_access_key"):
            get_session_from_direct_creds(cfg, "dev")

    def test_raises_without_access_key(self, tmp_path):
        from configparser import ConfigParser
        cfg = ConfigParser()
        cfg.read_string("[dev]\naws_secret_access_key: SK\n")
        with pytest.raises(ValueError, match="aws_access_key_id"):
            get_session_from_direct_creds(cfg, "dev")


# ─── get_session_from_profile ───────────────────────


class TestGetSessionFromProfile:

    def setup_method(self):
        _reset()

    def test_raises_without_profile(self):
        from configparser import ConfigParser
        cfg = ConfigParser()
        cfg.read_string("[dev]\n")
        with pytest.raises(ValueError, match="profile_name"):
            get_session_from_profile(cfg, "dev")

    def test_strips_profile_name(self):
        from configparser import ConfigParser
        cfg = ConfigParser()
        cfg.read_string("[dev]\nprofile_name: my_prof  \n")
        get_session_from_profile(cfg, "dev")
        _mock_boto3.Session.assert_called_once_with(profile_name="my_prof")


# ─── Env Var Precedence ────────────────────────────


class TestEnvVarPrecedence:

    def setup_method(self):
        _reset()

    def test_env_vars_skip_config_file(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "ENV_KEY"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "ENV_SECRET"
        try:
            get_aws_setup({"aws_config_file": "nonexistent.cfg", "aws_setup": "dev"})
            _mock_boto3.Session.assert_called_once_with()
        finally:
            _clear_env()


# ─── Error Handling ─────────────────────────────────


class TestErrors:

    def setup_method(self):
        _reset()
        _clear_env()

    def test_missing_config_file_raises(self):
        with pytest.raises(FileNotFoundError):
            get_aws_setup({"aws_config_file": "/nonexistent/path.cfg", "aws_setup": "dev"})

    def test_uses_correct_section(self, tmp_path):
        path = os.path.join(str(tmp_path), "aws_config.cfg")
        with open(path, "w") as f:
            f.write("[dev]\nprofile_name: dev_prof\n[prod]\nprofile_name: prod_prof\n")
        get_aws_setup({"aws_config_file": path, "aws_setup": "prod"})
        _mock_boto3.Session.assert_called_once_with(profile_name="prod_prof")

    def test_no_creds_no_profile_raises(self, tmp_path):
        cfg = _write_config(tmp_path)  # empty section
        with pytest.raises(ValueError, match="profile_name"):
            get_aws_setup({"aws_config_file": cfg, "aws_setup": "dev"})


# ─── test_aws_connection ───────────────────────────


class TestAwsConnection:

    def test_success(self):
        session = MagicMock()
        session.client.return_value.list_topics.return_value = {"Topics": []}
        verify_aws_connection(session)  # should not raise

    def test_raises_on_no_creds(self):
        session = MagicMock()
        session.client.return_value.list_topics.side_effect = _NoCredentialsError()
        with pytest.raises(Exception, match="not available"):
            verify_aws_connection(session)
