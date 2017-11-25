#!/bin/bash

# TODO: - aws s3 command below fails (Import error at from botocore.utils import is_json_value_header) for second deploy in the same cluster, probably due to sudo "pip install boto3==1.2.6" below.

# Parse arguments
s3_bucket="$1"
s3_bucket_scripts="$s3_bucket/scripts.tar.gz"

# Download compressed script tar file from S3 after deploy.py moved it from laptop to S3.
aws s3 cp $s3_bucket_scripts /home/hadoop/scripts.tar.gz
aws s3 cp "$s3_bucket/setup.sh" /home/hadoop/setup.sh  # Added for debugging purposes only
aws s3 cp "$s3_bucket/terminate_idle_cluster.sh" /home/hadoop/terminate_idle_cluster.sh  # Added for debugging purposes only

# Untar file
cd /home/hadoop/
mkdir -p app
tar zxvf "/home/hadoop/scripts.tar.gz" -C /home/hadoop/app/

# python can add a zip in path, so create one.
# TODO: remove dirty shortcut. Get zip file from the start instead of tar first.
cd /home/hadoop/app
zip -r scripts.zip .


# Install requirements for Python script
# sudo python2.7 -m pip install referer_parser
sudo pip install boto3==1.2.6
sudo pip install networkx==1.11
sudo pip install pandas==0.20.3
