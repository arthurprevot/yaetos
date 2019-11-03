import logging


def setup_logging(logger_name, default_level=logging.INFO):
    """
    Setup logging configuration
    Use in scripts: logger = setup_logging('Job')
    """
    format = "|%(asctime)s:%(levelname)s:%(name)s|%(message)s"  # Default:   format = "%(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=default_level, format=format, datefmt="%H:%M:%S")
    logging.getLogger('Deploy').setLevel(logging.INFO)
    logging.getLogger('botocore.credentials').setLevel(logging.CRITICAL)
    logging.getLogger('boto3.resources.action').setLevel(logging.CRITICAL)
    return logging.getLogger(logger_name)
