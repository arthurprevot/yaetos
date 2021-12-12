import pytest
from jobs.examples.ex1_frameworked_job import Job


class Test_Job(object):
    def test_transform(self, sc, sc_sql, ss, get_pre_jargs):
        some_events = ss.read.json(sc.parallelize([
            {'session_id': 1234, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1234, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1235, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1236, 'action': 'other',            'n_results': 1},
            ]))

        other_events = ss.read.json(sc.parallelize([
            {'session_id': 1234, 'other': 1},
            {'session_id': 1235, 'other': 1},
            {'session_id': 1236, 'other': 1},
            ]))

        expected = [
            {'session_id': 1234, 'count_events': 2},
            {'session_id': 1235, 'count_events': 1},
            ]

        loaded_inputs={'some_events': some_events, 'other_events':other_events}
        actual = Job(pre_jargs=get_pre_jargs(loaded_inputs.keys())).etl_no_io(sc, sc_sql, loaded_inputs=loaded_inputs)[0].toPandas().to_dict(orient='records')
        assert actual == expected
