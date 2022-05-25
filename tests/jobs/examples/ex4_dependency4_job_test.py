import pytest
from jobs.examples.ex4_dependency1_job import Job
from yaetos.etl_utils import Commandliner
from yaetos.pandas_utils import load_csvs
# from pandas.testing import assert_frame_equal
from yaetos.etl_utils import Path_Handler


def test_job(sc, sc_sql, ss, get_pre_jargs):
    expected = [
        {'session_id': 's1', 'session_length': 2},
        {'session_id': 's2', 'session_length': 2},
        {'session_id': 's3', 'session_length': 2},
        ]

    args = {
        'job_param_file': 'conf/jobs_metadata.yml',
        'base_path': './tests/fixtures/data/',
        'dependencies': False,
        'add_created_at':False,
        'skip_cmdline': True}
    Commandliner(Job, **args)  # -> drops files to path below
    path = args['base_path'] + 'wiki_example/output_ex4_dep1/{latest}/'
    path = Path_Handler(path, args['base_path']).expand_later(storage='n/a')

    actual = load_csvs(path, read_kwargs={}).to_dict(orient='records')
    # assert_frame_equal(actual, expected)

    assert actual == expected
