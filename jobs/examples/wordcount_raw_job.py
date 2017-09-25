from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext


inputs = "s3://bucket-scratch/wordcount_test/input/sample_text.txt"  # cluster
inputs = "sample_text.txt"  # local
output = "s3://bucket-scratch/wordcount_test/output/v4/"  # cluster
output = "tmp/output_v3/"  # local


# Start SparkContext
sc = SparkContext(appName="PythonWordCount")
# Load data from S3 bucket
lines = sc.textFile(inputs, 1)
# Calculate word counts
counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
output = counts.collect()
# Print word counts
for (word, count) in output:
    print("%s: %i" % (word, count))
# Save word counts in S3 bucket
counts.saveAsTextFile(output)
# Stop SparkContext
sc.stop()

# To run locally, put local i/o paths and run: python jobs/examples/ex3_raw_spark_job.py
# To run on cluster, put s3 i/o paths and run: python core/deploy.py jobs/examples/ex3_raw_spark_job.py
