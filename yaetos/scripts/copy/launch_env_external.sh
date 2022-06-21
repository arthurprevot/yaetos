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


yaetos_jobs_home=$PWD  # location of folder with jobs. In that config, framework is pip installed, and available in path.

run_mode=$1  # values: 1 (docker bash), 2 (docker jupyter), 3 (exec in docker), 4 (exec in terminal)
if [[ $run_mode = 1 ]]; then
  echo 'Starting docker with bash'
  docker build -t pyspark_container . # builds from Dockerfile
  docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -p 8888:8888 \
      -v $yaetos_jobs_home:/mnt/yaetos_jobs \
      -v $HOME/.aws:/.aws \
      -h spark \
      -w /mnt/yaetos_jobs/ \
      pyspark_container \
      bash
elif [[ $run_mode = 2 ]]; then
  echo 'Starting docker with jupyter notebooks'
  docker build -t pyspark_container . # builds from Dockerfile
  docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -p 8888:8888 \
      -v $yaetos_jobs_home:/mnt/yaetos_jobs \
      -v $HOME/.aws:/.aws \
      -h spark \
      -w /mnt/yaetos_jobs/ \
      pyspark_container \
      jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root
elif [[ $run_mode = 3 ]]; then
  cmd_str="${@:2}"
  echo "Starting the following job in docker: $cmd_str"
  docker build -t pyspark_container . # builds from Dockerfile
  docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -p 8888:8888 \
      -v $yaetos_jobs_home:/mnt/yaetos_jobs \
      -v $HOME/.aws:/.aws \
      -h spark \
      -w /mnt/yaetos_jobs/ \
      pyspark_container \
      $cmd_str
elif [[ $run_mode = 4 ]]; then
  cmd_str="${@:2}"
  echo "Executing command: $cmd_str"
  $cmd_str
else
  echo 'Incorrect argument, command ignored'
fi
