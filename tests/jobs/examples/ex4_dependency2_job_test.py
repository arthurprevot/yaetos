from jobs.examples.ex4_dependency2_job import Job
import pandas as pd


class Test_Job(object):
    def test_transform(self, get_pre_jargs):
        some_events = pd.DataFrame([
            {'session_id': 1, 'session_length': 1},
            {'session_id': 12, 'session_length': 2},
            {'session_id': 123, 'session_length': 3},
            {'session_id': 1234, 'session_length': 4},
        ])

        expected = [
            {'session_id': 1,    'session_length': 1, 'doubled_length': 2},  # noqa: E241
            {'session_id': 12,   'session_length': 2, 'doubled_length': 4},  # noqa: E241
            {'session_id': 123,  'session_length': 3, 'doubled_length': 6},  # noqa: E241
            {'session_id': 1234, 'session_length': 4, 'doubled_length': 8},
        ]

        loaded_inputs = {'some_events': some_events}
        actual = Job(pre_jargs=get_pre_jargs(loaded_inputs.keys())).etl_no_io(sc=None, sc_sql=None, loaded_inputs=loaded_inputs)[0].to_dict(orient='records')
        assert actual == expected
