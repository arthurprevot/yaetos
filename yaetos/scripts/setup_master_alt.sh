#!/bin/bash
# Script to configure master node on EMR cluster. (as an emr "step", not boostrap)


# Parse arguments
s3_bucket="$1"
s3_bucket_scripts="$s3_bucket/scripts.tar.gz"


# Update awscli here as required by "aws s3 cp ..."
# TODO: pip install -r requirements.txt # see related note in setup_nodes.sh
sudo pip-3.6 install awscli==1.16.67 # depends on botocore from 1.12.57
# sudo pip.3-6 install scikit-learn==0.20.0  # TODO: remove when using req file, TODO: fix dep and re-enable since needed for joblib
# sudo pip-3.6 install statsmodels==0.9.0  # TODO: remove when using req file
sudo pip-3.6 install kafka-python==1.4.7
sudo pip-3.6 install jsonschema==3.0.2
sudo pip-3.6 install cloudpathlib==0.7.0
# sudo pip-3.6 install s3fs==2022.5.0  # for saving to S3 with pandas. TODO: more validation, impacts boto versions
sudo pip-3.6 install pyarrow  # latest 8.0.0 not available in env
# sudo pip-3.6 install koalas==1.3.0  # fails installing now. TODO: check.
# DB and API libs
sudo pip-3.6 install openpyxl==3.0.9  # used to open excel files. TODO: put in a new requirement_extra.txt, since optional.
sudo pip-3.6 install soql==1.0.2  # Necessary for simple-salesforce
# sudo pip-3.6 install setuptools-rust==0.11.6  # Necessary for simple-salesforce
# sudo pip-3.6 install cryptography==3.3.1  # Necessary for simple-salesforce, to avoid simple-salesforce loading 3.4.4 causing pbs. 3.3.1 now associated to security issues.
# sudo pip-3.6 install simple-salesforce==1.10.1
sudo pip-3.6 install pymysql==0.9.3
sudo pip-3.6 install psycopg2-binary==2.8.5  # necesary for sqlalchemy-redshift, psycopg2==2.8.5 fails installing.
sudo pip-3.6 install sqlalchemy-redshift==0.7.7
sudo pip-3.6 install stripe==2.50.0
sudo pip-3.6 install requests_oauthlib==1.3.0
sudo pip-3.6 install duckdb==0.4.0
# TODO: check to replace all libs above to "pip-3.6 install yaetos" (to make it more consistent) while allowing bypassing it to quickly test new libs in EMR without having to deploy to pypi.


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

# Creating a zip to be used as package by pyspark
# TODO: could get zip file from the start instead of tar, untar and zip.
echo "Zipping job files"
cd /home/hadoop/app
zip -r scripts.zip .

# export PYSPARK_AWS_ETL_HOME=`pwd` # TODO: enable later to be avoid hardcoded path in etl_utils.py
# . setup_oracle.sh  # uncomment if needed.

python --version # shows in stderr, ->2.7.18 on emr-5.26.0, 2.7.16 on emr-6.0.0
python3 --version # shows in stdout, ->3.6.10 on emr-5.26.0, 3.7.4 on emr-6.0.0
echo "Done with setup_master.sh"
