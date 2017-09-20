from core.helpers import etl, launch
from conf.scheduling import schedule #_local as schedule  # TODO for testing
from operator import add

class wordcount(etl):

    INPUTS = {'lines':{'path':schedule['wordcount']['inputs']['lines']}}
    OUTPUT = {'path':schedule['wordcount']['output']}

    def run(self, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    launch(classname=wordcount, appName='wordcount', app_file='jobs/spark_example/wordcount_frameworked.py', aws='perso')
