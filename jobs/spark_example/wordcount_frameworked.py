from core.helpers import etl, launch
from conf.scheduling import schedule_local as schedule  # TODO for testing
from operator import add

class wordcount(etl):

    INPUTS = {'lines':schedule['wordcount']['inputs']['lines']}
    OUTPUT = schedule['wordcount']['output']

    def run(self, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    launch(job_class=wordcount, aws='perso')
