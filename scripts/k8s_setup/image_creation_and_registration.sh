#!/bin/bash

# Script to create and register images to AWS ECR. It needs to be executed from the folder containing this script
# Usage:
# - ./image_creation_and_registration.sh creation_repo  # to be done once per repo name
# - ./image_creation_and_registration.sh create_spark_img
# - ./image_creation_and_registration.sh login_ECR
# - ./image_creation_and_registration.sh push_to_ECR


cwd=$(pwd)  # PUT YOUR PATH if needed.
shared_folder="${cwd}/../../"
image_repo=pyspark_img
image_name=$image_repo:3.5.1
docker_file=Dockerfile_k8s_custom
run_mode=$1
aws_region=us-east-1

if [[ $run_mode = "creation_repo" ]]; then
  echo 'Creating repository in AWS'
  aws ecr create-repository --repository-name $image_repo
# -------
elif [[ $run_mode = "create_spark_img" ]]; then
  echo 'Create and get in docker'
  docker build -t $image_name  -f $docker_file .
  docker run -it \
      -v $shared_folder:/mnt/shared_folder \
      -h spark \
      -w /mnt/shared_folder/ \
      $image_name \
      bash
      # -v $HOME/.aws:/.aws \
# -------
elif [[ $run_mode = "login_ECR" ]]; then
  echo 'Create and get in docker'
  ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')
  aws ecr get-login-password --region $aws_region | docker login --username AWS --password-stdin "$ACCOUNT_ID".dkr.ecr.$aws_region.amazonaws.com
# -------
elif [[ $run_mode = "push_to_ECR" ]]; then
  echo 'Create and get in docker'
  ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')
  docker tag $image_name "$ACCOUNT_ID".dkr.ecr.$aws_region.amazonaws.com/$image_name
  docker push "$ACCOUNT_ID".dkr.ecr.$aws_region.amazonaws.com/$image_name
# -------
else
  echo 'Incorrect argument, command ignored'
fi
