"""
More integration-test than unit-test. This runs pipeline with 4 jobs, loading data from file system and dropping to file system for each one.
Code here reusable directly in jupyter notebooks.
"""
import pytest
from jobs.examples.ex4_dependency4_job import Job
from yaetos.etl_utils import Runner
from yaetos.pandas_utils import load_csvs


def test_job(sc, sc_sql, ss, get_pre_jargs):
    expected = [
        {'session_id': 's1', 'session_length': 2, 'doubled_length': 4, 'quadrupled_length': 8, 'D': 16.0},
        {'session_id': 's2', 'session_length': 2, 'doubled_length': 4, 'quadrupled_length': 8, 'D': 16.0},
        {'session_id': 's3', 'session_length': 2, 'doubled_length': 4, 'quadrupled_length': 8, 'D': 16.0}]

    # Generating 'actual'
    args = {
        'job_param_file': 'conf/jobs_metadata.yml',
        'base_path': './tests/fixtures/data_sample/',
        'dependencies': True,
        'add_created_at':False,
        'parse_cmdline': False}
    job = Runner(Job, **args).run()
    path = job.jargs.output['path_expanded']
    actual = load_csvs(path, read_kwargs={}).to_dict(orient='records')

    # Comparing
    assert actual == expected
