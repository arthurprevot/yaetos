# Setup for run in local mode
# Assumes spark (2.1.0) is already setup locally or in docker image


#apt install software-properties-common
#add-apt-repository ppa:deadsnakes/ppa
#apt install python3.7 # doesn't work.

mkdir -p tmp/files_to_ship/
pip3 install -r requirements.txt

# Set env variables.
# Requires having $SPARK_HOME set. Already set in docker image but may not be set if running outside of docker.
export PYSPARK_AWS_ETL_HOME=`pwd`
export PYTHONPATH=$PYSPARK_AWS_ETL_HOME:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH

#mkdir -p data
