from core.helpers import etl, launch
from conf.scheduling import schedule_local as schedule  # TODO for testing
from operator import add


class worksi_chat(etl):

    INPUTS = {'chats':schedule['worksi_chat']['inputs']['chats']}
    OUTPUT = schedule['worksi_chat']['output']

    def run(self, chats):
        chats_per_person = self.query("""
            SELECT actorRole, count(*) as chat_count
            from chats
            group by actorRole
            order by chat_count desc
            """)
        return chats_per_person


if __name__ == "__main__":
    launch(classname=worksi_chat, appName='worksi_chat', app_file='jobs/spark_example/worksi_chat.py', aws='perso')
