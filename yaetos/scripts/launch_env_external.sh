#!/bin/bash

# Script to enter the docker container with yaetos setup. It needs to be executed from the repo root folder.
# Note "-v ~/.aws:/.aws \" is necessary to run jobs in AWS (ad-hoc or scheduled). Requires awcli setup on host (with ~/.aws setup with profile "default").


yaetos_jobs_home=$PWD

docker build -t pyspark_container . # builds from Dockerfile
docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 \
    -v $yaetos_jobs_home:/mnt/yaetos_jobs \
    -v $HOME/.aws:/.aws \
    -h spark \
    -w /mnt/yaetos_jobs/ \
    pyspark_container
