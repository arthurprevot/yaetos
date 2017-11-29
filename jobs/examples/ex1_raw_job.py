"""
Same as ex1_full_sql_job.sql and ex1_frameworked_job, but with no helper functions.
The script is responsible for creating and closing spark context, defining pathsregistering tables in sparkSQL if needed...
"""

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from core.etl_utils import Path_Handler

# inputs/output paths
# input_some_events = "s3://bucket-scratch/bogus_data/inputs2/{latest}/events_log.csv.gz"  # cluster
input_some_events = "data/bogus_data/inputs/2017-01-02/events_log.csv.gz"  # local
# input_other_events = "s3://bucket-scratch/bogus_data/inputs2/events_log.csv.gz"  # cluster
input_other_events = "data/bogus_data/inputs/2017-01-02/events_log.csv.gz"  # local
# output = "s3://bucket-scratch/bogus_data_sql/output/{now}/"  # cluster
output = "data/bogus_data/output_ex1_raw/{now}/"  # local
output = Path_Handler(output).expand_now()


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

# To run locally, put local i/o paths and run: python jobs/examples/ex1_raw_job.py
# To run on cluster, put s3 i/o paths and run: python core/deploy.py jobs/examples/ex1_raw_job.py
