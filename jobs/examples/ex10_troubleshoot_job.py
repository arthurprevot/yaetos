"""To show a troubleshooting method, using 'import ipdb; ipdb.set_trace()' below."""
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

        import ipdb; ipdb.set_trace()  # will drop to python terminal here to inspect  # noqa: E702

        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
