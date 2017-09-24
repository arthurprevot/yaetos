from core.etl_utils import etl, launch
from operator import add


class wordcount(etl):

    def run(self, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    launch(job_class=wordcount, aws_setup='perso')
