import os
from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np
from yaetos.pandas_utils import load_csvs, load_df, save_pandas_local


def test_load_csvs():
    # Test multiple file option
    path = 'tests/fixtures/data_sample/wiki_example/input/'
    actual = load_csvs(path, read_kwargs={}).sort_values('uuid')
    expected = pd.DataFrame([
        {'uuid': 'u1', 'timestamp': 2.0, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u2', 'timestamp': 2.0, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u3', 'timestamp': 2.0, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
        {'uuid': 'u4', 'timestamp': 2.0, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u5', 'timestamp': 2.0, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u6', 'timestamp': 2.0, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
    ])
    assert_frame_equal(actual, expected)

    # Test single file option
    path = 'tests/fixtures/data_sample/wiki_example/input/part1.csv'
    actual = load_csvs(path, read_kwargs={})
    expected = pd.DataFrame([
        {'uuid': 'u1', 'timestamp': 2.0, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u2', 'timestamp': 2.0, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u3', 'timestamp': 2.0, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
    ])
    assert_frame_equal(actual, expected)


def test_load_df():
    # Test multiple file option
    path = 'tests/fixtures/data_sample/wiki_example/input/'
    actual = load_df(path, file_type='csv', read_func='read_csv', read_kwargs={}).sort_values('uuid')
    expected = pd.DataFrame([
        {'uuid': 'u1', 'timestamp': 2.0, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u2', 'timestamp': 2.0, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u3', 'timestamp': 2.0, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
        {'uuid': 'u4', 'timestamp': 2.0, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u5', 'timestamp': 2.0, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u6', 'timestamp': 2.0, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
    ])
    assert_frame_equal(actual, expected)

    # Test single file option, csv
    path = 'tests/fixtures/data_sample/wiki_example/input/part1.csv'
    actual = load_df(path, file_type='csv', read_func='read_csv', read_kwargs={})
    expected = pd.DataFrame([
        {'uuid': 'u1', 'timestamp': 2.0, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u2', 'timestamp': 2.0, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u3', 'timestamp': 2.0, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
    ])
    assert_frame_equal(actual, expected)

    # Test single file option, parquet
    path = 'tests/fixtures/data_sample/wiki_example/input_parquet/part1.parquet'
    actual = load_df(path, file_type='parquet', read_func='read_parquet', read_kwargs={})
    expected = pd.DataFrame([
        {'uuid': 'u1', 'timestamp': 2.0, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u2', 'timestamp': 2.0, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u3', 'timestamp': 2.0, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
    ])
    assert_frame_equal(actual, expected)

    # Test single file option, excel
    path = 'tests/fixtures/data_sample/wiki_example/input_excel/parts.xlsx'
    actual = load_df(path, file_type='xlsx', read_func='read_excel', read_kwargs={'engine': 'openpyxl', 'sheet_name': 0, 'header': 1})
    expected = pd.DataFrame([
        {'uuid': 'u1', 'timestamp': 2, 'session_id': 's1', 'group': 'g1', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p1', 'n_results': 5.0, 'result_position': np.nan},
        {'uuid': 'u2', 'timestamp': 2, 'session_id': 's2', 'group': 'g2', 'action': 'searchResultPage', 'checkin': np.nan, 'page_id': 'p2', 'n_results': 9.0, 'result_position': np.nan},
        {'uuid': 'u3', 'timestamp': 2, 'session_id': 's3', 'group': 'g3', 'action': 'checkin', 'checkin': 30, 'page_id': 'p3', 'n_results': np.nan, 'result_position': np.nan},
    ])
    assert_frame_equal(actual, expected)

# TODO Add test 'test_load_multiple_csvs())', for completeness. functionality already tested above..


def test_save_pandas_local():
    # Test save to csv
    path = 'tests/tmp/output.csv'
    df_in = pd.DataFrame([
        {'uuid': 'u1', 'timestamp': 2.0, 'session_id': 's1'},
        {'uuid': 'u2', 'timestamp': 2.0, 'session_id': 's2'},
        {'uuid': 'u3', 'timestamp': 2.0, 'session_id': 's3'},
    ])
    save_pandas_local(df_in, path, save_method='to_csv', save_kwargs={})
    assert os.path.exists(path)

    # Test save to parquet
    path = 'tests/tmp/output.parquet'
    save_pandas_local(df_in, path, save_method='to_parquet', save_kwargs={})
    assert os.path.exists(path)
