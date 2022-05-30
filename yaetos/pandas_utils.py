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
    # TODO: to be made obsolete once load_multiple_files works
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    df = pd.concat((pd.read_csv(f, **read_kwargs) for f in csv_files))
    return df.reset_index(drop=True)


def load_multiple_files(path, file_type='csv', read_func='read_csv', read_kwargs={}):
    files = glob.glob(os.path.join(path, "*.{}".format(file_type)))
    func = getattr(pd, read_func)
    df = pd.concat((func(f, **read_kwargs) for f in files))
    return df.reset_index(drop=True)


def load_csvs(path, read_kwargs):
    """Loading 1 csv or multiple depending on path"""
    # TODO: to be made obsolete once load_df works
    if path.endswith('.csv'):
        return pd.read_csv(path, **read_kwargs)
    elif path.endswith('/'):
        return load_multiple_csvs(path, read_kwargs)
    else:
        raise Exception("Path should end with '.csv' or '/'.".format())


def load_df(path, file_type='csv', read_func='read_csv', read_kwargs={}):
    """Loading 1 file or multiple depending on path"""
    if path.endswith(".{}".format(file_type)):
        func = getattr(pd, read_func)
        return func(path, **read_kwargs)
    elif path.endswith('/'):
        return load_multiple_files(path, file_type, read_func, read_kwargs)
    else:
        raise Exception("Path should end with '.{}' or '/'.".format(file_type))


# --- saving files ----

def create_subfolders(path):
    """Creates subfolders if needed. Can take a path with a file."""
    output_dir = Path(path).parent
    output_dir.mkdir(parents=True, exist_ok=True)


def save_pandas_csv_local(df, path):
    # TODO: to be made obsolete once save_pandas_local works
    create_subfolders(path)
    df.to_csv(path)


def save_pandas_local(df, path, save_method='to_csv', save_kwargs={}):
    if isinstance(path, str):  # to deal with case when path is StringIO
        create_subfolders(path)
    func = getattr(df, save_method)
    func(path, **save_kwargs)


if __name__ == '__main__':
    pass
