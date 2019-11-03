from core.etl_utils import ETL_Base, Commandliner
from sqlalchemy import types


class Job(ETL_Base):
    OUTPUT_TYPES = {
        'session_id': types.VARCHAR(16),
        'count_events': types.INT(),
        }

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
    Commandliner(Job, aws_setup='perso')
