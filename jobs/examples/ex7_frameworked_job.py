"""Created to show job with no output."""
from core.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events):
        return some_events


if __name__ == "__main__":
    args = {
        'job_param_file':   'conf/jobs_metadata.yml',  # Just to be explicit. Not needed since this is default.
        'connection_file':  'conf/connections.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_config_file':  'conf/aws_config.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_setup':        'dev',  # Just to be explicit. Not needed since this is default.
        'jobs_folder':      'jobs/',  # Just to be explicit. Not needed since this is default.
        }
    Commandliner(Job, **args)
