from core.helpers import etl, launch
from conf.scheduling import schedule_local as schedule  # TODO for testing
from operator import add


class worksi_user_dimension(etl):

    INPUTS = {'chats':schedule['worksi_chat']['inputs']['chats']}
    OUTPUT = schedule['worksi_chat']['output']

    def run(self, chats):
        tb = self.query("""
            SELECT actor, actorRole
            from chats
            group by actor, actorRole
            """)
        return tb


if __name__ == "__main__":
    launch(job_class=worksi_user_dimension, aws='perso')
