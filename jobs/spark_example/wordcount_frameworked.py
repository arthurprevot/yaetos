from core.helpers import etl
from operator import add
from conf.scheduling import schedule #_local as schedule  # TODO for testing

class wordcount(etl):

    INPUTS = {'lines':{'path':schedule['wordcount']['inputs']['lines']}}
    OUTPUT = {'path':schedule['wordcount']['output']}

    def run(self, sc, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    import sys
    process = 'run_local'
    if len(sys.argv) > 1:
        process = sys.argv[1]
    # data_location = sys.argv[2] # can be local or cluster.

    # sys.exit()
    if process == 'run_local':
        from pyspark import SparkContext
        sc = SparkContext(appName="PythonWordCount")
        wordcount().runner(sc)
    elif process == 'ship_cluster':
        from core.run import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(app_file="jobs/spark_example/wordcount_frameworked.py", setup='perso').run()

    # To run locally, set local path in schedule and run: python jobs/spark_example/wordcount_frameworked.py
    # To run on cluster, put s3 i/o paths in schedule and run: python core/run.py ## no easy way to ship to cluster from this script
