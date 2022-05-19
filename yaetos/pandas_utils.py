"""
Helper functions for pandas as engine.
"""

import pandas as pd
from pathlib import Path
from yaetos.logger import setup_logging
logger = setup_logging('pandas')


def create_subfolders(path):
    """Creates subfolders if needed. Can take a path with a file."""
    output_dir = Path(path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

def save_pandas(df, path):
    # Only works in local. TODO: add ability to push to S3
    create_subfolders(path)
    df.to_csv(path)


if __name__ == '__main__':
    pass
