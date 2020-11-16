"""Job to show loading mysql using spark connector.
Cred info and table defined in job_metadata.yml.
May require VPN to access mysql.
"""
from core.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events):
        return some_events


if __name__ == "__main__":
    args = {
        'job_param_file':   'conf/jobs_metadata_local.yml',  # Just to be explicit. Not needed since this is default.
        'connection_file':  'conf/connections.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_config_file':  'conf/aws_config.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_setup':        'dev',  # Just to be explicit. Not needed since this is default.
        'jobs_folder':      'jobs/',  # Just to be explicit. Not needed since this is default.
        }
    Commandliner(Job, **args)
