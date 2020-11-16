# To run jobs locally with spark-submit instead of running the python directly
# usage: ./spark_submit_local.sh jobs/examples/ex9_redshift_job.py
# Provided for testing purposed since it is the common way to launch spark apps. Problem: this doesn't support ipdb debugger and doesn't support adding python arguments (they need to be added inside jobs).
# Better alternative to run jobs in local with this framework is to use the standard python launcher "python some_job.py". It will integrate the required packages to spark too, and supports ipdb.

job=$1
export AWS_ACCESS_KEY_ID=`aws configure get default.aws_access_key_id`
export AWS_SECRET_ACCESS_KEY=`aws configure get default.aws_secret_access_key`
# Use `aws configure get aws_access_key_id --profile <new_profile>` to use different profile.
packages=com.amazonaws:aws-java-sdk-pom:1.11.760,org.apache.hadoop:hadoop-aws:2.7.0,com.databricks:spark-redshift_2.11:2.0.1,org.apache.spark:spark-avro_2.11:2.4.0,mysql:mysql-connector-java:8.0.11  # should be in sync with list in deploy.py
jars=https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.41.1065/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar  # should be in sync with list in deploy.py

spark-submit \
  --packages $packages \
  --jars $jars \
  $job

# TODO: check way to get arguments (ex: --storage=s3 --job_param_file=conf/jobs_metadata.yml) recognized
