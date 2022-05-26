FROM docker.io/bitnami/spark:3.1.2
USER root

# Pip installs
RUN apt-get update && apt-get install -y git
RUN pip3 install --no-deps yaetos==0.9.16
# Force latest version to avoid using previous ones.
RUN pip3 install -r /opt/bitnami/python/lib/python3.6/site-packages/yaetos/scripts/requirements_alt.txt
# TODO: check to put all yaetos requirements in package def to avoid having to call it separately.
# Uncomment 2 lines below to install extra packages. Requires creating a requirements_extra.txt in conf/ file. Using local copy to tmp dir to allow checkpointing this step (no re-installs as long as requirements.txt doesn't change)
# COPY conf/requirements_extra.txt /tmp/requirements_extra.txt
# RUN pip3 install -r /tmp/requirements_extra.txt

# Setup environment variables
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH

# Expose ports for monitoring.
# SparkContext web UI on 4040 -- only available for the duration of the application.
# Spark master’s web UI on 8080.
# Spark worker web UI on 8081.
# Jupyter web UI on 8888.
EXPOSE 4040 8080 8081 8888

# CMD ["/bin/bash"] # commented so the command can be sent by docker run

# Usage: docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -v ~/.aws:/root/.aws -h spark <image_id>
# or update launch_env.sh and execute it.
