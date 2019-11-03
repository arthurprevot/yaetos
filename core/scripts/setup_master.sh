#!/bin/bash
# Script to configure master node on EMR cluster.


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

#--------- Setup Oracle ----------
sudo yum install libaio -y
if [ ! -d /home/hadoop/conda ]; then
    PKG_URL=https://repo.continuum.io/miniconda/Miniconda2-4.5.12-Linux-x86_64.sh  # 4.5.4 works. later versions not tested.
    INSTALLER=miniconda.sh
    set -ex \
        && curl -kfSL $PKG_URL -o $INSTALLER \
        && chmod 755 $INSTALLER \
        && ./$INSTALLER -b -p /home/hadoop/conda \
        && rm $INSTALLER
        # can use ./$INSTALLER * -u * to force reinstall (implies remove check if dir exist)
fi

# didn't find a way to install oracle-instantclient from apt-get or yum
# so getting it from conda. Need to only put conda in PATH (/usr/local/bin/)
# to avoid python from conda taking over original python.
/home/hadoop/conda/bin/conda install -y -c trent oracle-instantclient=11.2.0.4.0  # full version is 11.2.0.4.0-1 but fails when adding the "-1". was "-c anaconda" until 24 oct 2019 and was pulling v11.2.0.4.0-0

sudo pip install cx_Oracle==7.2.3
ORACLE_HOME=/home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1
LD_LIBRARY_PATH=/home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib

sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libclntsh.so /usr/lib/
sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libnnz11.so /usr/lib/
sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libociei.so /usr/lib/
sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libclntsh.so.11.1 /usr/lib/
# only needed for debug: sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libsqlplus.so /usr/lib/
sudo ldconfig  # To confirm libs findable : "ldd $ORACLE_HOME/bin/sqlplus"
echo "Done with Oracle setup"
#--------- Setup Oracle ----------

echo "Done with setup_master.sh"
