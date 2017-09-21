from core.helpers import etl, launch #, load_schedule
# from conf.scheduling import schedule_local as schedule  # TODO for testing
from operator import add

# yml = load_schedule('conf/scheduling_local.yml')
# import ipdb; ipdb.set_trace()

class worksi_session_facts(etl):

    # import ipdb; ipdb.set_trace()
    # INPUTS = {'chats':schedule['worksi_chat']['inputs']['chats']}
    # OUTPUT = schedule['worksi_chat']['output']

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
