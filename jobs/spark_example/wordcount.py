#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    run_type = 'cluster'

    # Start SparkContext
    sc = SparkContext(appName="PythonWordCount")
    # Load data from S3 bucket
    if run_type == 'cluster':
        lines = sc.textFile("s3://bucket-scratch/wordcount_test/input/sample_text.txt", 1)
    elif run_type == 'local':
        lines = sc.textFile("sample_text.txt", 1)
    # Calculate word counts
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    # Print word counts
    for (word, count) in output:
        print("%s: %i" % (word, count))
    # Save word counts in S3 bucket
    if run_type == 'cluster':
        counts.saveAsTextFile("s3://bucket-scratch/wordcount_test/output/v3/")
    elif run_type == 'local':
        counts.saveAsTextFile("tmp/output_v3/")
    # Stop SparkContext
    sc.stop()

    # To run locally, put local i/o paths and run: python wordcount.py
    # To run on cluster, put s3 i/o paths and run: python core/run.py ## no easy way to ship to cluster from this script
