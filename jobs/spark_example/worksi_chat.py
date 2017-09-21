from core.helpers import etl, launch


class worksi_session_facts(etl):

    def run(self, chats):
        tb = self.query("""
            SELECT session_id, count(*)
            FROM chats
            WHERE action='searchResultPage' and n_results>0
            group by session_id
            order by count(*) desc
            """)
        return tb


if __name__ == "__main__":
    launch(job_class=worksi_session_facts, aws='perso')
