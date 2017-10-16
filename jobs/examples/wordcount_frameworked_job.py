from core.etl_utils import ETL_Base
from operator import add


class Job(ETL_Base):
    def transform(self, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    Job().commandline_launch(aws_setup='perso')
