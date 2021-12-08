import pytest
from core.etl_utils import ETL_Base, Commandliner, \
    Job_Args_Parser, Job_Yml_Parser, Flow, \
    get_job_class, LOCAL_APP_FOLDER


class Test_ETL_Base(object):
    def test_check_pk(self, sc, sc_sql, ss):
        sdf = ss.read.json(sc.parallelize([
            {'id': 1},
            {'id': 2},
            {'id': 3}]))

        pks = ['id']
        assert ETL_Base.check_pk(sdf, pks) is True

        sdf = ss.read.json(sc.parallelize([
            {'id': 1},
            {'id': 2},
            {'id': 2}]))

        pks = ['id']
        assert ETL_Base.check_pk(sdf, pks) is False

    # def test_set_py_job(self, get_pre_jargs):  # works locally but not from CI tool, where LOCAL_APP_FOLDER is different.
    #     py_job = ETL_Base(pre_jargs=get_pre_jargs({})).set_py_job()
    #     assert py_job == LOCAL_APP_FOLDER+'core/etl_utils.py' # file is the one that starts execution, typically the job python file.

    def test_load_inputs(self, sc, sc_sql, ss, get_pre_jargs):
        """Confirming load_inputs acts as a passthrough"""
        sdf = ss.read.json(sc.parallelize([
            {'id': 1},
            {'id': 2},
            {'id': 3}]))
        loaded_inputs = {'input1':sdf}
        app_args_expected = loaded_inputs
        assert ETL_Base(pre_jargs=get_pre_jargs(loaded_inputs.keys())).load_inputs(loaded_inputs) == app_args_expected

    def test_get_max_timestamp(self, sc, sc_sql, ss, get_pre_jargs):
        sdf = ss.read.json(sc.parallelize([
            {'id': 1, 'timestamp': '2020-01-01'},
            {'id': 2, 'timestamp': '2020-01-02'},
            {'id': 3, 'timestamp': '2020-01-03'}]))
        pre_jargs_over = {
            'defaults_args': {
                'inputs':{},
                'output': {'inc_field': 'timestamp', 'type':None}}}
        max_timestamp_expected = '2020-01-03'
        assert ETL_Base(pre_jargs=get_pre_jargs(pre_jargs_over=pre_jargs_over)).get_max_timestamp(sdf) == max_timestamp_expected


class Test_Job_Args_Parser(object):
    def test_no_param_override(self):
        defaults_args = {'mode': 'dev_local', 'deploy':'code', 'output': {'path':'n/a', 'type': 'csv'}}
        expected_args = {**{'inputs': {}, 'is_incremental': False}, **defaults_args}

        jargs = Job_Args_Parser(defaults_args=defaults_args, yml_args={}, job_args={}, cmd_args={})
        assert jargs.merged_args == expected_args


class Test_Job_Yml_Parser(object):
    def test_set_py_job_from_name(self):
        py_job = Job_Yml_Parser.set_py_job_from_name('some_job_name')
        assert py_job == 'jobs/some_job_name'

    def test_set_job_name_from_file(self):
        job_name = Job_Yml_Parser.set_job_name_from_file('jobs/some/file.py')
        assert job_name == 'some/file.py'

        job_name = Job_Yml_Parser.set_job_name_from_file(LOCAL_APP_FOLDER+'jobs/some/file.py')
        assert job_name == 'some/file.py'


def test_get_job_class():
    Job = get_job_class(py_job='jobs/examples/ex1_frameworked_job.py')  # must be real job
    assert issubclass(Job, ETL_Base)
