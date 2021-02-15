import pytest
from jobs.examples.ex1_frameworked_job import Job
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
    def internal_function(loaded_inputs):
        inputs  = {key: {'type':None} for key in loaded_inputs.keys()}
        pre_jargs = {
            'defaults_args': {
                'manage_git_info': False, 'mode':'dev_local', 'deploy':'none', 'job_param_file':None,
                'output': {'type':None},
                'inputs': inputs},
            'job_args':{},
            'cmd_args':{}}
        return pre_jargs
    return internal_function
