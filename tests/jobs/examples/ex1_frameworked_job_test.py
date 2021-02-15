import pytest
from jobs.examples.ex1_frameworked_job import Job


class Test_Job(object):
    def test_transform(self, sc, sc_sql, ss):
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

        pre_jargs = {'defaults_args':
                        {'manage_git_info': False, 'mode':'dev_local', 'deploy':'none', 'job_param_file':None,
                         'output': {'type':None},
                         'inputs': {'some_events':{'type':None}, 'other_events':{'type':None}}},
                     'job_args':{}, 'cmd_args':{}}
        actual = Job(pre_jargs=pre_jargs).etl_no_io(sc, sc_sql, loaded_inputs={'some_events': some_events, 'other_events':other_events})[0].toPandas().to_dict(orient='records')
        assert actual == expected
