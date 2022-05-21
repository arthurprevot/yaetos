"""
Helper functions for pandas as engine.
"""

import pandas as pd
import glob
import os
from pathlib import Path
from yaetos.logger import setup_logging
logger = setup_logging('pandas')

# --- loading files ----

def load_multiple_csvs(path, read_kwargs):
    # path = "dir/to/save/to"
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    df = pd.concat((pd.read_csv(f, **read_kwargs) for f in csv_files))
    return df.reset_index(drop=True)

def load_csvs(path, read_kwargs):
    """Loading 1 csv or multiple depending on path"""
    if path.endswith('.csv'):
        return pd.read_csv(path, **read_kwargs)
    elif path.endswith('/'):
        return load_multiple_csvs(path, read_kwargs)
    else:
        raise Exception("Path should end with '.csv' or '/'.".format())


def load_all_parquets(path):
    # path = "dir/to/save/to"
    parquet_files = glob.glob(os.path.join(path, "*.parquet"))
    df = pd.concat((pd.read_parquet(f) for f in parquet_files))
    return df

# --- saving files ----

def create_subfolders(path):
    """Creates subfolders if needed. Can take a path with a file."""
    # Only works in local. TODO: add ability to push to S3
    output_dir = Path(path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

def save_pandas(df, path):
    # Only works in local. TODO: add ability to push to S3
    create_subfolders(path)
    df.to_csv(path)


if __name__ == '__main__':
    pass
