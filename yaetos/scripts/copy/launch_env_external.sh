#!/bin/bash

# Script to enter the docker container with yaetos setup, or setup variabes to run outside docker. It needs to be executed from the repo root folder.
# Note "-v ~/.aws:/.aws \" is necessary to run jobs in AWS (ad-hoc or scheduled). Requires awcli setup on host (with ~/.aws setup with profile "default").


yaetos_jobs_home=$PWD

run_docker=1  # values: 0 or 1
if [ $run_docker = 1 ]; then
  # Launch docker
  docker build -t pyspark_container . # builds from Dockerfile
  docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 \
      -v $yaetos_jobs_home:/mnt/yaetos_jobs \
      -v $HOME/.aws:/.aws \
      -h spark \
      -w /mnt/yaetos_jobs/ \
      pyspark_container \
      bash
else
  # Set variables to run outside of docker. Main use case: running pandas jobs.
  export PYSPARK_AWS_ETL_HOME=$PWD'/'
  export PYTHONPATH=$PYSPARK_AWS_ETL_HOME:$PYTHONPATH
  echo 'Yaetos setup to work from OS repo (not in docker), if sent as "source launch_env.sh"' # export won't work if run as ./launch_env.sh (due to subshell).
  # Spark jobs can also be run outside of docker but it will require setting more variables.
fi
