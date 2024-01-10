FROM docker.io/bitnami/spark:3.5.0
USER root

# Lib installs
RUN apt update \
  && apt install -y git \
  && apt install -y g++ \
  && apt install -y nodejs
# 2 lines above for jupyterlab

RUN python -m pip install --upgrade pip
RUN pip3 install --no-deps yaetos==0.11.1
# Force latest version to avoid using previous ones.
RUN pip3 install -r /opt/bitnami/python/lib/python3.8/site-packages/yaetos/scripts/requirements_base.txt
# Installing libraries required by Yaetos and more. Using this since requirements_base.txt has exact versions.
COPY conf/requirements_extra.txt /tmp/requirements_extra.txt
RUN pip3 install -r /tmp/requirements_extra.txt

# Setup environment variables
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
ENV YAETOS_JOBS_HOME /mnt/yaetos_jobs/

# Expose ports for monitoring.
# SparkContext web UI on 4040 -- only available for the duration of the application.
# Spark master’s web UI on 8080.
# Spark worker web UI on 8081.
# Jupyter web UI on 8888.
EXPOSE 4040 8080 8081 8888

# CMD ["/bin/bash"] # commented so the command can be sent by docker run

# Usage: docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -v ~/.aws:/root/.aws -h spark <image_id>
# or update launch_env.sh and execute it.
