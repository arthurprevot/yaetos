from yaetos.sql_spark_job import Job
import textwrap


class Test_Job(object):
    def test_set_jargs(self, get_pre_jargs):
        loaded_inputs = {}
        pre_jargs = get_pre_jargs(input_names=loaded_inputs.keys())
        pre_jargs['cmd_args']['sql_file'] = 'tests/fixtures/sql_job.sql'
        job = Job(pre_jargs=pre_jargs)
        actual = job.set_jargs(pre_jargs, loaded_inputs)

        expected = {
            'some_sql_file_param': 'some_value',
            'manage_git_info': False,
            'mode': 'dev_local',
            'deploy': 'none',
            'job_param_file': None,
            'output': {'type': None},
            'inputs': {},
            'job_name': 'tests/fixtures/sql_job.sql',
            'py_job': 'jobs/tests/fixtures/sql_job.sql',
            'sql_file': 'tests/fixtures/sql_job.sql',
            'is_incremental': False}
        assert actual.merged_args == expected

    def test_get_params_from_sql(self):
        sql = textwrap.dedent("""
        ----param---- 'param_a': 'value_a' ----
        ----param---- 'param_to_be_ignored': 'some_value' --
        ---param--- 'param_to_be_ignored': 'some_value' ----
        """)
        expected = {'param_a': 'value_a'}
        actual = Job.get_params_from_sql(sql)
        assert actual == expected
