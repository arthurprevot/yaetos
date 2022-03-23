"""Job to showcase use of mysql job base class. Using pandas/sqlalchemy connector.
Requires access to a mysql instance and credentials setup in connection file."""
from yaetos.etl_utils import Commandliner
from yaetos.mysql_job import Mysql_Job
from sqlalchemy import types


class Job(Mysql_Job):
    OUTPUT_TYPES = {
        'some_field1': types.INT(),
        'some_field2': types.INT(),
        'some_field3': types.VARCHAR(100)}

    def transform(self):
        return self.query_mysql("""SELECT * FROM some_table """)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
