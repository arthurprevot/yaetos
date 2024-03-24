"""
Helper functions for pandas as engine.
"""
import pandas as pd
import glob
import json
import os
from pathlib import Path
from yaetos.logger import setup_logging
logger = setup_logging('Pandas')
try:
    import duckdb
    DUCKDB_SETUP = True
except ModuleNotFoundError or ImportError:
    logger.debug("DuckDB not found. Yaetos won't be able to run SQL jobs on top of pandas.")
    DUCKDB_SETUP = False


# --- loading files ----

def load_multiple_csvs(path, read_kwargs):
    # TODO: to be made obsolete once load_multiple_files works
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    df = pd.concat((pd.read_csv(f, **read_kwargs) for f in csv_files))
    return df.reset_index(drop=True)


def load_multiple_files(path, globy='*.csv', read_func='read_csv', read_kwargs={}, add_file_fol=True):
    files = glob.glob(os.path.join(path, globy))  # removes local files
    files = [file for file in files if not os.path.basename(file).startswith('.')]  # remove sys files added by OS, ex .DS_store in mac.

    dfs = []
    for fi in files:
        df = load_df(fi, read_func, read_kwargs)
        if add_file_fol:
            df['_source'] = fi
        dfs.append(df)
    df = pd.concat(dfs) if len(dfs) >= 1 else pd.DataFrame()
    return df.reset_index(drop=True)


def load_csvs(path, read_kwargs):
    """Loading 1 csv or multiple depending on path"""
    # TODO: to be made obsolete once load_dfs works
    if path.endswith('.csv'):
        return pd.read_csv(path, **read_kwargs)
    elif path.endswith('/'):
        return load_multiple_csvs(path, read_kwargs)
    else:
        raise Exception("Path should end with '.csv' or '/'.".format())


def load_dfs(path, file_type='csv', globy=None, read_func='read_csv', read_kwargs={}):
    """Loading 1 file or multiple depending on path"""
    if globy:
        # TODO: improve check, make it usable with several files.
        matching_paths = glob.glob(globy)
        matches_pattern = os.path.normpath(path) in map(os.path.normpath, matching_paths)
        is_file = len(matching_paths) == 1

    if path.endswith(".{}".format(file_type)):  # one file and extension is explicite
        return load_df(path, read_func, read_kwargs)
    elif globy and matches_pattern and is_file:  # one file and extension is not explicite
        return load_df(path, read_func, read_kwargs)
    elif path.endswith('/'):  # multiple files.
        globy = globy or f'*.{file_type}'
        return load_multiple_files(path, globy, read_func, read_kwargs)
    else:  # case where file has no extension. TODO: make above more generic by using glob.
        raise Exception("Path should end with '.{}' or '/'.".format(file_type))


def load_df(path, read_func='read_csv', read_kwargs={}):
    """Loading 1 file or multiple depending on path"""
    if read_func != 'json_parser':
        func = getattr(pd, read_func)
        return func(path, **read_kwargs)
    else:
        with open(path, 'r') as file:
            data = json.load(file)

        df = pd.DataFrame(data['records'])
        return df


# --- saving files ----

def create_subfolders(path):
    """Creates subfolders if needed. Can take a path with a file."""
    output_dir = Path(path).parent
    output_dir.mkdir(parents=True, exist_ok=True)


def save_pandas_csv_local(df, path):
    # TODO: to be made obsolete once save_pandas_local works
    create_subfolders(path)
    df.to_csv(path, float_format='%.2f'.replace('.', ','))


def save_pandas_local(df, path, save_method='to_csv', save_kwargs={}):
    if isinstance(path, str):  # to deal with case when path is StringIO
        create_subfolders(path)
    func = getattr(df, save_method)
    func(path, **save_kwargs)


# --- other ----

def query_pandas(query_str, dfs):
    assert DUCKDB_SETUP is True
    con = duckdb.connect(database=':memory:')
    for key, value in dfs.items():
        con.register(key, value)
    df = con.execute(query_str).df()
    return df
