"""
Same as ex1_full_sql_job.sql and ex1_frameworked_job, but with no helper functions.
The script is responsible for creating and closing spark context, defining pathsregistering tables in sparkSQL if needed...
"""

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from datetime import datetime

# inputs/output paths
input_some_events = "s3://data-bi-dwh-dev/yaetos/wiki_example/inputs/2017-01-01/events_log.csv.gz"  # cluster
input_other_events = "s3://data-bi-dwh-dev/yaetos/wiki_example/inputs/2017-01-02/events_log.csv.gz"  # cluster
output = "s3://data-bi-dwh-dev/yaetos/wiki_example/output_ex1_raw/v3/"  # TODO: make sure path doesn't exist or it will crash.

# Start SparkContext
sc = SparkContext(appName='ex1_raw_job')
sc_sql = SQLContext(sc)

# Load data from S3 bucket
some_events = sc_sql.read.csv(input_some_events, header=True)
some_events.createOrReplaceTempView('some_events')
other_events = sc_sql.read.csv(input_other_events, header=True)
other_events.createOrReplaceTempView('other_events')

# Calculate word counts
query_str = """
    SELECT se.session_id, count(*)
    FROM some_events se
    JOIN other_events oe on se.session_id=oe.session_id
    WHERE se.action='searchResultPage' and se.n_results>0
    group by se.session_id
    order by count(*) desc
    """

df = sc_sql.sql(query_str)

# Save word counts in S3 bucket
df.write.csv(output)

# Stop SparkContext
sc.stop()
print('Done checking pyspark')

# Check that we can load other libraries
import pandas # basic data manipulation
import numpy # basic data manipulation
import sklearn # ML
import statsmodels # ML
import cx_Oracle # to push to oracle
import sqlalchemy # to push to oracle and to convert pandas DF to spark DF
import kafka # to push to kafka
import jsonschema # to push to kafka
print('Done checking loading python libs')
# pip install awscli==1.16.67 # depends on botocore from 1.12.57
# pip install pandas==0.20.3
# pip install scikit-learn==0.20.0
# pip install statsmodels==0.9.0
# pip install cx_Oracle  # no version specified
# pip install sqlalchemy==1.1.13
# pip install kafka-python==1.4.7
# pip install jsonschema==3.0.2

# To run locally, put local i/o paths and run: python jobs/examples/ex1_raw_job.py
# To run on cluster, put s3 i/o paths and run: python core/deploy.py jobs/examples/ex1_raw_job_cluster.py
