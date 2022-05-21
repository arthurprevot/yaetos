import pytest
from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np
from yaetos.pandas_utils import load_multiple_csvs, load_csvs

def tes_load_multiple_csvs():
    path = 'fixtures/csvs/'
    df = load_multiple_csvs(path, read_kwargs={})

def test_load_csvs():
    path = 'tests/fixtures/csvs/'
    actual = load_csvs(path, read_kwargs={})
    expected = pd.DataFrame([
        {'uuid':'u1','timestamp':2.0,'session_id':'s1','group': 'g1','action':'searchResultPage','checkin':np.nan,'page_id':'p1','n_results':5.0,'result_position':np.nan},
        {'uuid':'u2','timestamp':2.0,'session_id':'s2','group': 'g2','action':'searchResultPage','checkin':np.nan,'page_id':'p2','n_results':9.0,'result_position':np.nan},
        {'uuid':'u3','timestamp':2.0,'session_id':'s3','group': 'g3','action':'checkin','checkin':30,         'page_id':'p3','n_results':np.nan,'result_position':np.nan},
        {'uuid':'u4','timestamp':2.0,'session_id':'s1','group': 'g1','action':'searchResultPage','checkin':np.nan,'page_id':'p1','n_results':5.0,'result_position':np.nan},
        {'uuid':'u5','timestamp':2.0,'session_id':'s2','group': 'g2','action':'searchResultPage','checkin':np.nan,'page_id':'p2','n_results':9.0,'result_position':np.nan},
        {'uuid':'u6','timestamp':2.0,'session_id':'s3','group': 'g3','action':'checkin','checkin':30,         'page_id':'p3','n_results':np.nan,'result_position':np.nan},
        ])
    assert_frame_equal(actual, expected)

    path = 'tests/fixtures/csvs/part1.csv'
    actual = load_csvs(path, read_kwargs={})
    expected = pd.DataFrame([
        {'uuid':'u1','timestamp':2.0,'session_id':'s1','group': 'g1','action':'searchResultPage','checkin':np.nan,'page_id':'p1','n_results':5.0,'result_position':np.nan},
        {'uuid':'u2','timestamp':2.0,'session_id':'s2','group': 'g2','action':'searchResultPage','checkin':np.nan,'page_id':'p2','n_results':9.0,'result_position':np.nan},
        {'uuid':'u3','timestamp':2.0,'session_id':'s3','group': 'g3','action':'checkin','checkin':30,         'page_id':'p3','n_results':np.nan,'result_position':np.nan},
        ])
    assert_frame_equal(actual, expected)
