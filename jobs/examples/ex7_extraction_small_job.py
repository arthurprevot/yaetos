"""Job to get small version of wiki sample data, to speed up running downstream jobs, for testing purposes."""
from yaetos.etl_utils import ETL_Base, Commandliner

# TODO: move it to .sql job.
class Job(ETL_Base):
    def transform(self, events):
        df = self.query("""
            SELECT *
            FROM events
            LIMIT 1000
            """)
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
