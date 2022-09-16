from jobs.examples.ex1_pandas_api_job import Job
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

        loaded_inputs = {'some_events': some_events, 'other_events': other_events}
        actual = Job(pre_jargs=get_pre_jargs(loaded_inputs.keys())).etl_no_io(sc=None, sc_sql=None, loaded_inputs=loaded_inputs)[0].to_dict(orient='records')
        assert actual == expected
