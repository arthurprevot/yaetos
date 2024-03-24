# Setup for run in local mode
# to be run from repo root.
# Not needed when running from docker since included there.
# Assumes spark is already setup locally or in docker image

mkdir -p tmp/files_to_ship/
pip3 install -r yaetos/scripts/requirements_base.txt

# Set env variables.
# Requires having $SPARK_HOME set. Already set in docker image but may not be set if running outside of docker.
export PYSPARK_AWS_ETL_HOME=`pwd`'/'
export PYTHONPATH=$PYSPARK_AWS_ETL_HOME:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
export PYSPARK_AWS_ETL_JOBS_HOME='put the path to your external repo here'
