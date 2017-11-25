#!/bin/bash

# Parse arguments
s3_bucket="$1"
s3_bucket_script="$s3_bucket/script.tar.gz"  #TODO update to scripts.tar.gz

# Download compressed script tar file from S3 after deploy.py moved it from laptop to S3.
aws s3 cp $s3_bucket_script /home/hadoop/script.tar.gz
# aws s3 cp "$s3_bucket/setup.sh" /home/hadoop/setup.sh  # Add later for debugging purposes only

# Untar file
cd /home/hadoop/
mkdir -p app
tar zxvf "/home/hadoop/script.tar.gz" -C /home/hadoop/app/

# python can add a zip in path, so create one.
# TODO: remove dirty shortcut. Get zip file from the start instead of tar first.
cd /home/hadoop/app
zip -r scripts.zip .


# Install requirements for Python script
# sudo python2.7 -m pip install referer_parser
sudo pip install boto3==1.2.6
sudo pip install networkx==1.11
sudo pip install pandas==0.20.3
