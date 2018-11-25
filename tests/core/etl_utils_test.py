import pytest
from core.etl_utils import ETL_Base, Commandliner, LOCAL_APP_FOLDER

class Test_ETL_Base(object):

    def test_set_job_file(self):
        # Check file set as arg
        my_etl = ETL_Base({'job_file':'some/file.py'})
        my_etl.set_job_file()
        assert my_etl.job_file == 'some/file.py'

        # Check file infered
        my_etl = ETL_Base()
        my_etl.set_job_file()
        assert my_etl.job_file.replace(LOCAL_APP_FOLDER, '') == 'core/etl_utils.py' # file is the one that starts execution, typically the job python file.

    def test_set_job_name(self):
        my_etl = ETL_Base()
        my_etl.set_job_name('jobs/some/file.py')
        assert my_etl.job_name == 'some/file.py'

        my_etl.set_job_name(LOCAL_APP_FOLDER+'jobs/some/file.py')
        assert my_etl.job_name == 'some/file.py'

    def test_get_job_class(self):
        Job = ETL_Base.get_job_class(job_name='examples/ex1_frameworked_job.py')  # must be real job
        assert issubclass(Job, ETL_Base)
