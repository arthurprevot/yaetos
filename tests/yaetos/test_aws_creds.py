"""Unit tests for get_aws_setup() — direct credentials and profile-based auth.

These tests mock boto3 at the sys.modules level to avoid requiring it installed locally.
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch


# ── Mock boto3 before importing yaetos.etl_utils ──
_mock_boto3 = MagicMock()
_mock_session = MagicMock()
_mock_creds = MagicMock()
_mock_creds.access_key = "DEFAULT_KEY"
_mock_creds.secret_key = "DEFAULT_SECRET"
_mock_creds.token = ""
_mock_session.get_credentials.return_value = _mock_creds
_mock_boto3.Session.return_value = _mock_session

# Pre-inject mocks for modules that etl_utils imports
# Mock ALL heavy dependencies that etl_utils transitively imports
_heavy_deps = [
    "boto3", "botocore", "botocore.exceptions",
    "networkx", "cloudpathlib", "pandas", "pyspark",
    "pyspark.sql", "pyspark.sql.types", "pyspark.sql.functions",
    "pyspark.ml", "pyspark.ml.feature",
    "dateutil", "dateutil.relativedelta",
    "smtplib", "zipfile",
    "yaetos.spark_utils", "yaetos.pandas_utils",
    "yaetos.git_utils", "yaetos.env_dispatchers",
    "yaetos.logger",
]
for mod_name in _heavy_deps:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

sys.modules["boto3"] = _mock_boto3

# Mock the logger setup
mock_logger = MagicMock()
sys.modules["yaetos.logger"].setup_logging = MagicMock(return_value=mock_logger)

# Now we can import get_aws_setup
from yaetos.aws_creds import get_aws_setup, get_session_from_direct_creds, get_session_from_profile
from yaetos.aws_creds import test_aws_connection as verify_aws_connection


def _reset_boto3_mock():
    """Reset boto3 mock state between tests."""
    _mock_boto3.Session.reset_mock()
    _mock_creds.access_key = "DEFAULT_KEY"
    _mock_creds.secret_key = "DEFAULT_SECRET"
    _mock_creds.token = ""


def _write_config(tmp_path, section="dev", access_key=None, secret_key=None,
                  session_token=None, region=None, profile_name=None):
    """Helper to create a temp aws_config.cfg file."""
    config_path = os.path.join(str(tmp_path), "aws_config.cfg")
    lines = [f"[{section}]"]
    if access_key:
        lines.append(f"aws_access_key_id    : {access_key}")
    if secret_key:
        lines.append(f"aws_secret_access_key: {secret_key}")
    if session_token:
        lines.append(f"aws_session_token    : {session_token}")
    if region:
        lines.append(f"s3_region            : {region}")
    if profile_name:
        lines.append(f"profile_name         : {profile_name}")
    with open(config_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return config_path


def _clear_aws_env():
    """Remove AWS env vars to avoid short-circuiting."""
    for k in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
        os.environ.pop(k, None)


# ─── Direct Credentials Tests ──────────────────────


class TestDirectCreds:

    def setup_method(self):
        _reset_boto3_mock()
        _clear_aws_env()

    def test_creates_session_with_keys(self, tmp_path):
        """Direct credentials should create a boto3 session with explicit keys."""
        config_path = _write_config(tmp_path, access_key="AKIATEST1234", secret_key="SECRET1234")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        _mock_boto3.Session.assert_called_once_with(
            aws_access_key_id="AKIATEST1234",
            aws_secret_access_key="SECRET1234",
            aws_session_token=None,
            region_name=None,
        )

    def test_passes_session_token(self, tmp_path):
        """Session token should be forwarded when provided."""
        config_path = _write_config(tmp_path, access_key="AKIA", secret_key="SEC", session_token="TOK123")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        call_kwargs = _mock_boto3.Session.call_args[1]
        assert call_kwargs["aws_session_token"] == "TOK123"

    def test_passes_region(self, tmp_path):
        """Region from s3_region should be forwarded."""
        config_path = _write_config(tmp_path, access_key="AKIA", secret_key="SEC", region="eu-west-1")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        call_kwargs = _mock_boto3.Session.call_args[1]
        assert call_kwargs["region_name"] == "eu-west-1"

    def test_sets_env_vars(self, tmp_path):
        """Should populate env vars for downstream use (Spark, etc.)."""
        _mock_creds.access_key = "ENVKEY"
        _mock_creds.secret_key = "ENVSEC"
        _mock_creds.token = "ENVTOK"

        config_path = _write_config(tmp_path, access_key="AKIA", secret_key="SEC")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        assert os.environ["AWS_ACCESS_KEY_ID"] == "ENVKEY"
        assert os.environ["AWS_SECRET_ACCESS_KEY"] == "ENVSEC"
        assert os.environ["AWS_SESSION_TOKEN"] == "ENVTOK"

    def test_strips_whitespace(self, tmp_path):
        """Should strip whitespace from credentials."""
        config_path = _write_config(tmp_path, access_key="  AKIA_SPACED  ", secret_key="  SEC_SPACED  ")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        call_kwargs = _mock_boto3.Session.call_args[1]
        assert call_kwargs["aws_access_key_id"] == "AKIA_SPACED"
        assert call_kwargs["aws_secret_access_key"] == "SEC_SPACED"


# ─── Profile Fallback Tests ────────────────────────


class TestProfileFallback:

    def setup_method(self):
        _reset_boto3_mock()
        _clear_aws_env()

    def test_uses_profile_when_no_direct_creds(self, tmp_path):
        """When no direct creds, should fall back to profile_name."""
        config_path = _write_config(tmp_path, profile_name="my_profile")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        _mock_boto3.Session.assert_called_once_with(profile_name="my_profile")

    def test_direct_creds_take_precedence(self, tmp_path):
        """Direct creds should be used even when profile is also present."""
        config_path = _write_config(tmp_path, access_key="AKIA", secret_key="SEC", profile_name="ignored")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        call_kwargs = _mock_boto3.Session.call_args[1]
        assert call_kwargs["aws_access_key_id"] == "AKIA"
        assert "profile_name" not in call_kwargs

    def test_only_access_key_falls_back_to_profile(self, tmp_path):
        """If only access_key (no secret), should fall back to profile."""
        config_path = _write_config(tmp_path, access_key="AKIAONLY", profile_name="fallback")
        get_aws_setup({"aws_config_file": config_path, "aws_setup": "dev"})

        _mock_boto3.Session.assert_called_once_with(profile_name="fallback")


# ─── Env Var Precedence Tests ──────────────────────


class TestEnvVarPrecedence:

    def setup_method(self):
        _reset_boto3_mock()

    def test_env_vars_skip_config_file(self):
        """When env vars are set, should use them (no config file read)."""
        os.environ["AWS_ACCESS_KEY_ID"] = "ENV_KEY"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "ENV_SECRET"

        try:
            session = get_aws_setup({"aws_config_file": "nonexistent.cfg", "aws_setup": "dev"})
            _mock_boto3.Session.assert_called_once_with()
        finally:
            _clear_aws_env()


# ─── Error Handling Tests ──────────────────────────


class TestErrors:

    def setup_method(self):
        _reset_boto3_mock()
        _clear_aws_env()

    def test_missing_config_file_raises(self):
        """Should raise FileNotFoundError if config file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            get_aws_setup({"aws_config_file": "/nonexistent/path.cfg", "aws_setup": "dev"})

    def test_uses_correct_section(self, tmp_path):
        """Should read from the specified section."""
        config_path = os.path.join(str(tmp_path), "aws_config.cfg")
        with open(config_path, "w") as f:
            f.write("[dev]\nprofile_name : dev_profile\n[prod]\nprofile_name : prod_profile\n")

        get_aws_setup({"aws_config_file": config_path, "aws_setup": "prod"})
        _mock_boto3.Session.assert_called_once_with(profile_name="prod_profile")
