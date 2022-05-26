#!/bin/bash

# Script to setup the environment for yaetos. It needs to be executed from the repo root folder.
# Before using:
# - make sure awcli is setup on host (with ~/.aws setup with profile "default"). It involved running "pip install awcli; aws configure"
# - if not, remove : "-v $HOME/.aws:/.aws \" to use the tool in local mode only. It won't be able to interact with AWS.
#
# Usage:
# - "./launch_env.sh" -> no docker container, can be used to run pandas jobs or to run jobs on AWS.
# - "./launch_env.sh 1" -> goes in docker bash, can be used to run all jobs in command line (incl. spark jobs)
# - "./launch_env.sh 2" -> sets up jupyter in docker, can be used to run all jobs in jupyter. Open UI in host OS at http://localhost:8888/
# Note "-v ~/.aws:/.aws \" is necessary to run jobs in AWS (ad-hoc or scheduled). Requires awcli setup on host (with ~/.aws setup with profile "default").


yaetos_jobs_home=$PWD

run_docker=$1  # values: 0 (no docker) or 1 (docker bash), or 2 (docker jupyter)
if [[ $run_docker = 1 ]]; then
  echo 'About to run docker with bash'
  docker build -t pyspark_container . # builds from Dockerfile
  docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -p 8888:8888 \
      -v $yaetos_jobs_home:/mnt/yaetos_jobs \
      -v $HOME/.aws:/.aws \
      -h spark \
      -w /mnt/yaetos_jobs/ \
      pyspark_container \
      bash
elif [[ $run_docker = 2 ]]; then
  echo 'About to run docker with jupyter notebooks'
  docker build -t pyspark_container . # builds from Dockerfile
  docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -p 8888:8888 \
      -v $yaetos_jobs_home:/mnt/yaetos_jobs \
      -v $HOME/.aws:/.aws \
      -h spark \
      -w /mnt/yaetos_jobs/ \
      pyspark_container \
      jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root
else
  # Set variables to run outside of docker. Main use case: running pandas jobs.
  export PYSPARK_AWS_ETL_HOME=$PWD'/'
  export PYTHONPATH=$PYSPARK_AWS_ETL_HOME:$PYTHONPATH
  echo 'Yaetos setup to work from OS repo (not in docker)' # may need to be run as "source launch_env.sh". export may not work if run as ./launch_env.sh (due to subshell).
  # Spark jobs can also be run outside of docker but it will require setting more variables.
fi
