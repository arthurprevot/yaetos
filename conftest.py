import pytest
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row

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
        inputs  = {key: {'type':None} for key in input_names}
        pre_jargs = {
            'defaults_args': {
                'manage_git_info': False,
                'mode':'dev_local',
                'deploy':'none',
                'job_param_file':None,
                'output': {'type':None},
                'inputs': inputs},
            'job_args':{},
            'cmd_args':{}}
        if 'defaults_args' in pre_jargs_over.keys():
            pre_jargs['defaults_args'].update(pre_jargs_over['defaults_args'])
        else:
            pre_jargs.update(pre_jargs_over)
        return pre_jargs
    return internal_function
