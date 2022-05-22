"""Same as ex1_full_sql_job.sql but allows access to spark for more complex ops (not used here but in ex2_frameworked_job.py)."""
from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, events):
        df = self.query("""
            SELECT *
            FROM events
            LIMIT 1000
            """)
        return df


if __name__ == "__main__":
    # args = {'job_param_file': 'conf/jobs_metadata.yml'}
    args = {'job_param_file': 'conf/jobs_metadata.yml',
        'base_path': 's3://dev-spark2/yaetos'}
    Commandliner(Job, **args)
