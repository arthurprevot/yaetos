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
