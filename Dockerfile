FROM arthurpr/pyspark_aws_etl:latest  # also available arthurpr/pyspark_aws_etl:oracle to skip oracle install steps below.
USER root
WORKDIR /mnt/pyspark_aws_etl

ENV PYSPARK_AWS_ETL_HOME /mnt/pyspark_aws_etl
ENV PYTHONPATH $PYSPARK_AWS_ETL_HOME:$PYTHONPATH
# ENV SPARK_HOME /usr/local/spark # already set in base docker image
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH


# Install oracle
RUN apt-get install libaio1 -y
ENV PKG_URL https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
ENV INSTALLER miniconda.sh
RUN set -ex \
    && curl -kfSL $PKG_URL -o $INSTALLER \
    && chmod 755 $INSTALLER \
    && ./$INSTALLER -b -p /opt/conda \
    && rm $INSTALLER

# didn't find a way to install oracle-instantclient from apt-get
# so getting it from conda. Need to only put conda in PATH (/usr/local/bin/)
# to avoid python from conda taking over original python.
RUN ln -s /opt/conda/bin/conda /usr/local/bin/conda
RUN conda install -y oracle-instantclient

RUN pip install cx_Oracle
ENV ORACLE_HOME=/opt/conda/pkgs/oracle-instantclient-11.2.0.4.0-0
ENV LD_LIBRARY_PATH=/opt/conda/pkgs/oracle-instantclient-11.2.0.4.0-0/lib

RUN ln -s /opt/conda/pkgs/oracle-instantclient-11.2.0.4.0-0/lib/libclntsh.so /usr/lib/
RUN ln -s /opt/conda/pkgs/oracle-instantclient-11.2.0.4.0-0/lib/libnnz11.so /usr/lib/
RUN ln -s /opt/conda/pkgs/oracle-instantclient-11.2.0.4.0-0/lib/libociei.so /usr/lib


# Expose ports for monitoring.
# SparkContext web UI on 4040 -- only available for the duration of the application.
# Spark master’s web UI on 8080.
# Spark worker web UI on 8081.
EXPOSE 4040 8080 8081

CMD ["/bin/bash"]

# Usage: docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -v ~/code/pyspark_aws_etl:/mnt/pyspark_aws_etl -v ~/.aws:/root/.aws -h spark <image_id>
