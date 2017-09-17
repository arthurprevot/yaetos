from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
# from core.run import DeployPySparkScriptOnAws
from helpers import etl

class wordcount(etl):
    def run(self, sc, lines):
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        return counts


sc = SparkContext(appName="PythonWordCount")
wordcount().runner(sc)

# To run locally, set local path in schedule and run: python jobs/spark_example/wordcount_frameworked.py
# To run on cluster, put s3 i/o paths in schedule and run: python core/run.py ## no easy way to ship to cluster from this script
