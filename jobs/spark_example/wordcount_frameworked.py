#from __future__ import print_function
# import sys
from operator import add
from core.helpers import etl

class wordcount(etl):
    # INPUTS = {'path':}
    def run(self, sc, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


if __name__ == "__main__":
    import sys
    process = sys.argv[1]
    # data_location = sys.argv[2] # can be local or cluster.

    # sys.exit()
    if process == 'run_local':
        from pyspark import SparkContext
        from core.run import DeployPySparkScriptOnAws
        sc = SparkContext(appName="PythonWordCount")
        wordcount().runner(sc)
    elif process == 'ship_cluster':
        DeployPySparkScriptOnAws(app_file="jobs/spark_example/wordcount_frameworked.py", setup='perso').run()

    # To run locally, set local path in schedule and run: python jobs/spark_example/wordcount_frameworked.py
    # To run on cluster, put s3 i/o paths in schedule and run: python core/run.py ## no easy way to ship to cluster from this script
