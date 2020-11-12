# To run jobs locally with spark-submit instead of running the python directly
# usage: ./spark_submit_local.sh jobs/examples/ex9_redshift_job.py
# pros: adds extra libraries to allow access to S3 data from local and push data to redshift with spark connector (vpn required).
# cons: can't put debug checkpoints (ipdb) in the middle of the code to debut it + less intuitive than running python file directly + ignores script arguments so they should be forced inside script.

job=$1
export AWS_ACCESS_KEY_ID=`aws configure get default.aws_access_key_id`
export AWS_SECRET_ACCESS_KEY=`aws configure get default.aws_secret_access_key`
# Use `aws configure get aws_access_key_id --profile <new_profile>` to use different profile.
spark-submit \
  --packages com.amazonaws:aws-java-sdk-pom:1.11.760,org.apache.hadoop:hadoop-aws:2.7.0,com.databricks:spark-redshift_2.11:2.0.1,org.apache.spark:spark-avro_2.11:2.4.0,mysql:mysql-connector-java:8.0.11 \
  --jars https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.41.1065/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar \
  $job

  # --packages com.amazonaws:aws-java-sdk-pom:1.11.760,org.apache.hadoop:hadoop-aws:2.7.0,com.databricks:spark-redshift_2.11:2.0.1,org.apache.spark:spark-avro_2.11:2.4.0,mysql:mysql-connector-java:5.1.39 \


# TODO: check way to get arguments (ex: --storage=s3 --job_param_file=conf/jobs_metadata.yml) recognized
