# from yaetos.libs.pytest_utils.conftest import sc, sc_sql, \
#     ss, get_pre_jargs
# TODO fix renable code above and delete code below (duplicated from yaetos/libs/pytest*/conftest.py) when works in github actions.
# code above probably works when getting code from pip install yaetos but not from orig repo, so need to have it work in both cases.


import pytest
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession


@pytest.fixture(scope="session")
def sc(request):
    sc = SparkContext(appName='test')
    return sc


@pytest.fixture(scope="session")
def sc_sql(sc):
    return SQLContext(sc)


@pytest.fixture(scope="session")
def ss(sc):  # TODO: check if sc necessary here since not used downstream
    return SparkSession.builder.appName('test').getOrCreate()


@pytest.fixture
def get_pre_jargs():
    def internal_function(input_names=[], pre_jargs_over={}):
        """Requires loaded_inputs or pre_jargs_over"""
        inputs = {key: {'type': None} for key in input_names}
        pre_jargs = {
            'defaults_args': {
                'manage_git_info': False,
                'mode': 'dev_local',
                'deploy': 'none',
                'job_param_file': None,
                'output': {'type': None},
                'inputs': inputs},
            'job_args': {},
            'cmd_args': {}}
        if 'defaults_args' in pre_jargs_over.keys():
            pre_jargs['defaults_args'].update(pre_jargs_over['defaults_args'])
        else:
            pre_jargs.update(pre_jargs_over)
        return pre_jargs
    return internal_function


@pytest.fixture
def deploy_args():
    return {'aws_setup': 'dev',
            'aws_config_file': 'conf/aws_config.cfg'}


@pytest.fixture
def app_args():
    return {'py_job': 'some/job.py', 'job_name': 'some_job_name'}
