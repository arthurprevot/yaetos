#!/bin/bash
# Script to configure master node on EMR cluster. (as an emr "step", not boostrap)

# Parse arguments
s3_bucket="$1"
s3_bucket_scripts="$s3_bucket/scripts.tar.gz"
echo "--- S3 path to grab files: ", $s3_bucket

# Copy compressed script tar file from S3 to EMR master, after deploy.py moved it from laptop to S3.
echo "--- Copy S3 to EMR master ---"
aws s3 cp $s3_bucket_scripts /home/hadoop/scripts.tar.gz  # TODO check step worked or exit with failure, instead of failing silently.
aws s3 cp "$s3_bucket/setup_master.sh" /home/hadoop/setup_master.sh  # Added for debugging purposes only
aws s3 cp "$s3_bucket/setup_nodes.sh" /home/hadoop/setup_nodes.sh  # Added for debugging purposes only
aws s3 cp "$s3_bucket/requirements.txt" /home/hadoop/requirements.txt
aws s3 cp "$s3_bucket/requirements_extra.txt" /home/hadoop/requirements_extra.txt
aws s3 cp "$s3_bucket/terminate_idle_cluster.sh" /home/hadoop/terminate_idle_cluster.sh

# Install pip libs.
cd /home/hadoop/
sudo pip3 install --upgrade pip
echo "--- Installing requirements.txt ---"
sudo pip3 install -r requirements.txt
# Note: saw issues with libs from requirement not installed (because of other version already available).
# May need to force them using "sudo pip3 install --ignore-installed somelib==x.x.x". TODO: double check.
echo "--- Installing requirements_extra.txt ---"
sudo pip3 install -r requirements_extra.txt
echo "--- Checking versions ---"
echo "- pyyaml ---"
sudo pip3 show pyyaml
echo "- awscli ---"
sudo pip3 show awscli
echo "- boto3 ---"
sudo pip3 show boto3
echo "- pandas ---"
sudo pip3 show pandas
echo "- cloudpathlib ---"
sudo pip3 show cloudpathlib
echo "- duckdb ---"
sudo pip3 show duckdb


# Untar file
echo "--- Untaring job files ---"
mkdir -p app
tar zxvf "/home/hadoop/scripts.tar.gz" -C /home/hadoop/app/

# Creating a zip to be used as package by pyspark
# TODO: could get zip file from the start instead of tar, untar and zip.
echo "--- Zipping job files ---"
cd /home/hadoop/app
zip -r scripts.zip .

# export PYSPARK_AWS_ETL_HOME=`pwd` # TODO: enable later to be avoid hardcoded path in etl_utils.py
# . setup_oracle.sh  # uncomment if needed.

python --version # shows in stderr, ->2.7.18 on emr-5.26.0, 2.7.16 on emr-6.0.0, 2.7.18 on emr-6.1.1
python3 --version # shows in stdout, ->3.6.10 on emr-5.26.0, 3.7.4 on emr-6.0.0, 3.7.10 on emr-6.1.1
echo "Done with setup_master.sh"
