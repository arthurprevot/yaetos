import pytest
from core.etl_utils import ETL_Base, Job_Yml_Parser, Flow, Commandliner, LOCAL_APP_FOLDER

class Test_ETL_Base(object):

    def test_set_job_file_infered(self):
        job_file = ETL_Base().set_job_file(job_file=None)
        assert job_file.replace(LOCAL_APP_FOLDER, '') == 'core/etl_utils.py' # file is the one that starts execution, typically the job python file.

    def test_copy_to_oracle(self, sc, sc_sql, ss):
        df = ss.read.json(sc.parallelize([
            {'field1': 1234, 'field2': 'some_text', 'field3': 1, 'field4': 1},
            {'field1': 1234, 'field2': 'some_text', 'field3': 1, 'field4': 1},
            ]))
        types = {
                'field1': types.BigInteger(),
                'field2': types.VARCHAR(10),
                'field3': types.INT(),
                'field4': types.DATE(),
                }


        job_file = ETL_Base().copy_to_oracle(output, types)
        assert job_file.replace(LOCAL_APP_FOLDER, '') == 'core/etl_utils.py' # file is the one that starts execution, typically the job python file.

class Test_Job_Yml_Parser(object):
    def test_set_job_name(self):
        yml = Job_Yml_Parser()
        yml.set_job_name('jobs/some/file.py')
        assert yml.job_name == 'some/file.py'

        yml.set_job_name(LOCAL_APP_FOLDER+'jobs/some/file.py')
        assert yml.job_name == 'some/file.py'

class Test_Flow(object):
    def test_get_job_class(self):
        Job = Flow.get_job_class(py_job='jobs/examples/ex1_frameworked_job.py')  # must be real job
        assert issubclass(Job, ETL_Base)
