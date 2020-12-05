"""Created to show job with no output."""
from core.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events):
        return some_events


if __name__ == "__main__":
    args = {'job_param_file':   'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
