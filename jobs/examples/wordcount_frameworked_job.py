from core.etl_utils import etl_base
from operator import add


class Job(etl_base):
    def transform(self, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    Job().commandline_launch(aws_setup='perso')
