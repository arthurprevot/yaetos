#!/bin/bash
# Script to configure master node on EMR cluster. (as an emr "step", not boostrap)


# Parse arguments
s3_bucket="$1"
s3_bucket_scripts="$s3_bucket/scripts.tar.gz"

# Update awscli here as required by "aws s3 cp ..."
# TODO: pip install -r requirements.txt, cleaner but may make EMR longer to boot.
sudo pip install awscli==1.16.67 # depends on botocore from 1.12.57
sudo pip install scikit-learn==0.20.0  # TODO: remove when using req file
sudo pip install statsmodels==0.9.0  # TODO: remove when using req file
sudo pip install kafka-python==1.4.7
sudo pip install jsonschema==3.0.2

# Copy compressed script tar file from S3 to EMR master, after deploy.py moved it from laptop to S3.
echo "Copy S3 to EMR master"
aws s3 cp $s3_bucket_scripts /home/hadoop/scripts.tar.gz  # TODO check step worked or exit with failure, instead of failing silently.
aws s3 cp "$s3_bucket/setup_master.sh" /home/hadoop/setup_master.sh  # Added for debugging purposes only
aws s3 cp "$s3_bucket/setup_nodes.sh" /home/hadoop/setup_nodes.sh  # Added for debugging purposes only
aws s3 cp "$s3_bucket/terminate_idle_cluster.sh" /home/hadoop/terminate_idle_cluster.sh  # Added for debugging purposes only

# Untar file
echo "Untaring job files"
cd /home/hadoop/
mkdir -p app
tar zxvf "/home/hadoop/scripts.tar.gz" -C /home/hadoop/app/

# python can add a zip in path, so create one.
# TODO: remove dirty shortcut. Get zip file from the start instead of tar first.
echo "Zipping job files"
cd /home/hadoop/app
zip -r scripts.zip .

# export PYSPARK_AWS_ETL_HOME=`pwd` # TODO: enable later to be avoid hardcoded path in etl_utils.py
# . setup_oracle.sh  # uncomment if needed.

echo "Done with setup_master.sh"