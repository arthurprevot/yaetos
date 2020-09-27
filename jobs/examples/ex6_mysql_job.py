"""Job to showcase use of mysql job base class. Requires access to a mysql instance and credentials setup in connection file."""
from core.etl_utils import Commandliner
from core.mysql_job import Mysql_Job
from sqlalchemy import types


class Job(Mysql_Job):

    OUTPUT_TYPES = {
        'some_field1': types.INT(),
        'some_field2': types.INT(),
        'some_field3': types.VARCHAR(100),
        }

    def transform(self):
        query_str = """
            select *
            FROM some_table
            """
        return self.query_mysql(query_str)


if __name__ == "__main__":
    args = {
        'job_param_file':   'conf/jobs_metadata.yml',  # Just to be explicit. Not needed since this is default.
        'connection_file':  'conf/connections.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_config_file':  'conf/aws_config.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_setup':        'dev',  # Just to be explicit. Not needed since this is default.
        'jobs_folder':      'jobs/',  # Just to be explicit. Not needed since this is default.
        }
    Commandliner(Job, **args)
