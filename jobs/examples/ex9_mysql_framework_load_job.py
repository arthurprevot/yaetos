"""Job to show loading mysql using spark connector.
Cred info and table defined in job_metadata.yml.
May require VPN to access mysql.
"""
from core.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events):
        return some_events


if __name__ == "__main__":
    args = {'job_param_file':   'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
