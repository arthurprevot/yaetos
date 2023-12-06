from jobs.generic.sql_pandas_job import Job
import pandas as pd


class Test_Job(object):
    def test_transform(self, get_pre_jargs):
        some_events = pd.DataFrame([
            {'session_id': 1234, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1234, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1235, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1236, 'action': 'other',            'n_results': 1},  # noqa: E241
        ])

        other_events = pd.DataFrame([
            {'session_id': 1234, 'other': 1},
            {'session_id': 1235, 'other': 1},
            {'session_id': 1236, 'other': 1},
        ])

        expected = [
            {'session_id': 1234, 'count_events': 2},
            {'session_id': 1235, 'count_events': 1},
            # only diff with ex1_framework_job is session_id being str instead of int.
        ]

        sql_file = 'jobs/examples/ex1_sql_job.sql'

        # Get actual
        loaded_inputs = {'some_events': some_events, 'other_events': other_events}
        pre_jargs = get_pre_jargs(loaded_inputs.keys())
        pre_jargs['cmd_args']['sql_file'] = sql_file
        job = Job(pre_jargs=pre_jargs)
        assert 'param_a' in job.jargs.merged_args.keys()
        actual = job.etl_no_io(sc=None, sc_sql=None, loaded_inputs=loaded_inputs)[0].to_dict(orient='records')

        # Compare
        assert actual == expected
