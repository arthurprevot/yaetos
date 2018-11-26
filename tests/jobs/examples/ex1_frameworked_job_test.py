import pytest
from jobs.examples.ex1_frameworked_job import Job


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession, Row
sc = SparkContext(appName='test')
sc_sql = SQLContext(sc)
spark = SparkSession.builder.appName('test').getOrCreate()


class Test_Job(object):
    def test_transform(self):
        some_events = spark.read.json(sc.parallelize([
            {'session_id': 1234, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1234, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1235, 'action': 'searchResultPage', 'n_results': 1},
            {'session_id': 1236, 'action': 'other',            'n_results': 1},
            ]))
        other_events = spark.read.json(sc.parallelize([
            {'session_id': 1234, 'other': 1},
            {'session_id': 1235, 'other': 1},
            {'session_id': 1236, 'other': 1},
            ]))
        expected = [
            {'session_id': 1234, 'count_events': 2},
            {'session_id': 1235, 'count_events': 1},
            ]

        args = {'job_params_file': False,
                'input_some_events': 'n/a',
                'input_other_events': 'n/a',
                'output':'n/a',
                'frequency': 'n/a'}
        myjob = Job(args)
        myjob.sc = sc
        myjob.sc_sql = sc_sql
        myjob.app_name = sc.appName
        myjob.set_job_params()
        loaded_datasets = myjob.load_inputs({'some_events': some_events, 'other_events':other_events})
        actual = myjob.transform(**loaded_datasets).toPandas().to_dict(orient='records')  #.toJSON().collect()
        assert actual == expected

# run with
#>ArtTop:pyspark_aws_etl$ py.test tests/jobs/examples/ex1_frameworked_job_test.py
# or
#Test_Job().test_transform()
#>ArtTop:pyspark_aws_etl$ py.test tests/jobs/examples/ex1_frameworked_job_test.py
