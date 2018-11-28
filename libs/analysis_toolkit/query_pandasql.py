import pandas as pd
import pandasql as pdsql


def query_pandasql(query_str, fname, **kwargs):
    """Nice and fast but doesn't support windowing functions (sqlite limitation)."""
    df = pd.read_csv(fname, **kwargs)
    df_out = pdsql.sqldf(query_str, {'df':df})
    return df_out
