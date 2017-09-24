from core.etl_utils import etl, launch


class worksi_session_facts(etl):

    def run(self, some_events, other_events):
        tb = self.query("""
            SELECT se.session_id, count(*)
            FROM some_events se
            JOIN other_events oe on se.session_id=oe.session_id
            WHERE se.action='searchResultPage' and se.n_results>0
            group by se.session_id
            order by count(*) desc
            """)
        return tb


if __name__ == "__main__":
    launch(job_class=worksi_session_facts, aws_setup='perso')
