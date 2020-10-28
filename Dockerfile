FROM jupyter/pyspark-notebook
# TODO: build spark image from vanilla ubuntu (or other), see https://github.com/masroorhasan/docker-pyspark
# FROM arthurpr/pyspark_aws_etl:latest
# FROM arthurpr/pyspark_aws_etl:oracle # also available to skip oracle install steps below.
# USER root

# Pip installs. Using local copy to tmp dir to allow checkpointing this step (no re-installs as long as requirements.txt doesn't change)
RUN conda remove PyYAML --yes --offline
# command above only necessary when using "jupyter/pyspark-notebook" image base, which uses conda as package manager and pip cannot uninstall version.
COPY requirements.txt /tmp/requirements.txt
WORKDIR /tmp/
RUN pip3 install -r requirements.txt

WORKDIR /mnt/pyspark_aws_etl

# RUN mkdir -p tmp/files_to_ship/  # skipped, causes problems with permissions, whether run from root or jovyan user. Will need to be run manually once.
ENV PYSPARK_AWS_ETL_HOME /mnt/pyspark_aws_etl/
ENV PYTHONPATH $PYSPARK_AWS_ETL_HOME:$PYTHONPATH
# ENV SPARK_HOME /usr/local/spark # already set in base docker image
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH

ENV PYSPARK_AWS_ETL_JOBS_HOME /mnt/external_pipelines/
ENV PYTHONPATH $PYSPARK_AWS_ETL_JOBS_HOME:$PYTHONPATH
# or replace "/mnt/external_pipelines/" by the name of your external repo to make it match.
# TODO: fix external shared folders that can't be access from mac with current version of docker, but works from ubuntu.

# Expose ports for monitoring.
# SparkContext web UI on 4040 -- only available for the duration of the application.
# Spark master’s web UI on 8080.
# Spark worker web UI on 8081.
EXPOSE 4040 8080 8081

CMD ["/bin/bash"]

# Usage: docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -v ~/code/pyspark_aws_etl:/mnt/pyspark_aws_etl -v ~/.aws:/root/.aws -h spark <image_id>
# or update launch_env.sh and execute it.
