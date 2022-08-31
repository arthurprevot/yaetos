"""
Job to showcase loading file as pandas dataframe instead of spark dataframe.
This allows for faster run for small datasets but looses some of the benefits of spark dataframes (support for SQL, better field type management, etc.).
Transformation is the same as ex1_sql_job.sql
"""
from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events, other_events):
        some_events = some_events[:1000]  # to speed up since it is an example job
        query_str = """
        SELECT se.session_id, count(*) as count_events
        FROM some_events se
        JOIN other_events oe on se.session_id=oe.session_id
        WHERE se.action='searchResultPage' and se.n_results>0
        group by se.session_id
        order by count(*) desc
        """
        dfs = {'some_events': some_events, 'other_events': other_events}
        df = self.query(query_str, engine='pandas', dfs=dfs)
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
