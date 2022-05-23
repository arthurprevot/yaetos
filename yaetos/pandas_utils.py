"""
Helper functions for pandas as engine.
"""

import pandas as pd
import glob
import os
from pathlib import Path
from yaetos.logger import setup_logging
logger = setup_logging('Pandas')

# --- loading files ----

def load_multiple_csvs(path, read_kwargs):
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

#def load_df(load_type, path, load_function, read_kwargs):

# --- saving files ----

def create_subfolders(path):
    """Creates subfolders if needed. Can take a path with a file."""
    output_dir = Path(path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

def save_pandas_csv_local(df, path):
    create_subfolders(path)
    df.to_csv(path)


if __name__ == '__main__':
    pass
