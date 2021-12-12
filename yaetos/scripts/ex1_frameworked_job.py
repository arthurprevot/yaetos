"""Same as ex1_full_sql_job.sql but allows access to spark for more complex ops (not used here but in ex2_frameworked_job.py)."""
from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events, other_events):
        df = self.query("""
            SELECT se.session_id, count(*) as count_events
            FROM some_events se
            JOIN other_events oe on se.session_id=oe.session_id
            WHERE se.action='searchResultPage' and se.n_results>0
            group by se.session_id
            order by count(*) desc
            """)
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
