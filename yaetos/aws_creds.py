"""
AWS credentials management for Yaetos.

Two authentication methods:
  Option 1 — Direct credentials in aws_config.cfg (aws_access_key_id, aws_secret_access_key)
  Option 2 — AWS profile name (profile_name) referencing ~/.aws/credentials

Both methods export credentials to environment variables for downstream use (Spark, etc.).
"""
import os
import boto3
from configparser import ConfigParser
from yaetos.logger import setup_logging

logger = setup_logging('AWSCreds')


def get_aws_setup(args):
    """
    Create a boto3 session using the best available credentials.

    Priority:
      1. Environment variables (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY already set)
      2. Direct credentials in aws_config.cfg
      3. AWS profile name in aws_config.cfg

    Args:
        args: dict with 'aws_config_file' and 'aws_setup' (section name)

    Returns:
        boto3.Session
    """
    # Check env vars first
    if os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'):
        session = boto3.Session()
        logger.info("AWS session created from environment variables")
        return session

    # Read config file
    config = _read_config(args['aws_config_file'])
    section = args['aws_setup']

    # Try direct credentials, fallback to profile
    aws_access_key = config.get(section, 'aws_access_key_id', fallback=None)
    aws_secret_key = config.get(section, 'aws_secret_access_key', fallback=None)

    if aws_access_key and aws_secret_key:
        session = get_session_from_direct_creds(config, section)
    else:
        session = get_session_from_profile(config, section)

    # Export to env for downstream (Spark, etc.)
    _export_credentials_to_env(session)
    return session


def get_session_from_direct_creds(config, section):
    """
    Option 1: Create a boto3 session from direct credentials in aws_config.cfg.

    Expected config fields:
      aws_access_key_id     : (required)
      aws_secret_access_key : (required)
      aws_session_token     : (optional, for STS/temporary credentials)
      s3_region             : (optional)

    Args:
        config: ConfigParser instance
        section: config section name (e.g. 'dev', 'prod')

    Returns:
        boto3.Session
    """
    aws_access_key = config.get(section, 'aws_access_key_id', fallback=None)
    aws_secret_key = config.get(section, 'aws_secret_access_key', fallback=None)

    if not aws_access_key or not aws_secret_key:
        raise ValueError(
            f"Direct credentials require both 'aws_access_key_id' and "
            f"'aws_secret_access_key' in [{section}] of the config file."
        )

    aws_session_token = config.get(section, 'aws_session_token', fallback=None)
    region = config.get(section, 's3_region', fallback=None)

    session = boto3.Session(
        aws_access_key_id=aws_access_key.strip(),
        aws_secret_access_key=aws_secret_key.strip(),
        aws_session_token=aws_session_token.strip() if aws_session_token else None,
        region_name=region.strip() if region else None,
    )
    logger.info(f"AWS session created from direct credentials in [{section}]")
    return session


def get_session_from_profile(config, section):
    """
    Option 2: Create a boto3 session from an AWS profile name.

    The profile references credentials stored in ~/.aws/credentials
    (managed by 'aws configure' or manually).

    Expected config field:
      profile_name : (required)

    Args:
        config: ConfigParser instance
        section: config section name (e.g. 'dev', 'prod')

    Returns:
        boto3.Session
    """
    profile_name = config.get(section, 'profile_name', fallback=None)

    if not profile_name:
        raise ValueError(
            f"No credentials found in [{section}]: need either "
            f"'aws_access_key_id'+'aws_secret_access_key' (direct) "
            f"or 'profile_name' (profile-based)."
        )

    session = boto3.Session(profile_name=profile_name.strip())
    logger.info(f"AWS session created from profile '{profile_name.strip()}'")
    return session


def test_aws_connection(session):
    """
    Verify that the AWS session has valid credentials by making a simple API call.

    Raises:
        Exception on credential or connection errors.
    """
    from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

    try:
        sns_client = session.client('sns')
        _ = sns_client.list_topics()
        logger.info("AWS connection test: successful")
    except NoCredentialsError:
        raise Exception("AWS credentials not available")
    except PartialCredentialsError:
        raise Exception("AWS credentials incomplete")
    except ClientError as e:
        raise Exception(f"AWS connection error: {e}")


def _read_config(config_file):
    """Read and return a ConfigParser from the given file."""
    config = ConfigParser()
    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"AWS config file not found: {config_file}")
    config.read(config_file)
    return config


def _export_credentials_to_env(session):
    """Export session credentials to environment variables for downstream use."""
    credentials = session.get_credentials()
    if credentials:
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key or ''
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key or ''
        os.environ['AWS_SESSION_TOKEN'] = credentials.token or ''
