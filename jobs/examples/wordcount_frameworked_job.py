from core.etl_utils import etl
from operator import add


class wordcount_frameworked_job(etl):
    def transform(self, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    wordcount_frameworked_job().commandline_launch(aws_setup='perso')
