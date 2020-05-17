#!/bin/bash
# Setup example data for local and cluster runs. Needs to be run from repo root.

# --------------------------------------
# Manual steps: create sandbox bucket and put it's name below if necessary
bucket_name="sandbox-spark"
# --------------------------------------

# General setup
# . setup.sh # TODO check if needs to be uncommented.

# copy data locally, will be used for local sample tests
local_folder="data/wiki_example/inputs/2017-01-01"
mkdir -p $local_folder
local_dataset="$local_folder/events_log.csv.gz"
if [ ! -f $local_dataset ]; then
    echo "Downloading: " + $local_dataset
    curl https://raw.githubusercontent.com/wikimedia-research/Discovery-Hiring-Analyst-2016/master/events_log.csv.gz -o $local_dataset
fi

## copy dataset to new timestamp folder
local_latest_folder="data/wiki_example/inputs/2017-01-02"
mkdir -p $local_latest_folder
local_latest_dataset="$local_latest_folder/events_log.csv.gz"
cp $local_dataset $local_latest_dataset

## copy dataset to new timestamp folder and under different name to pretend it is other dataset.
local_latest_dataset="$local_latest_folder/other_events_log.csv.gz"
cp $local_dataset $local_latest_dataset


# copy data to s3, will be used for cluster sample tests
## pre data copy

if ! aws s3api head-bucket --bucket $bucket_name --profile $1 2>/dev/null; then
  echo "S3 Bucket '$bucket_name' doesn't exist. Please create it in AWS and rerun this script."
  return
fi
s3_bucket="s3://$bucket_name"
s3_folder="$s3_bucket/yaetos/wiki_example/inputs/2017-01-01"

## copy dataset to "$s3_folder/events_log.csv.gz"
s3_dataset="$s3_folder/events_log.csv.gz"
echo "Putting dataset ($local_dataset) in S3 ($s3_dataset) if not already done."
aws s3 sync $local_folder $s3_folder --exclude '*' --include 'events_log.csv.gz' --profile $1

## copy dataset to new timestamp folder
s3_latest_folder="$s3_bucket/yaetos/wiki_example/inputs/2017-01-02"
s3_latest_dataset="$s3_latest_folder/events_log.csv.gz"
echo "Copying it to ($s3_latest_dataset) if not already done."
aws s3 cp $s3_dataset $s3_latest_dataset --profile $1

## copy dataset to new timestamp folder and under different name to pretend it is other dataset.
s3_latest_folder="$s3_bucket/yaetos/wiki_example/inputs/2017-01-02"
s3_latest_dataset="$s3_latest_folder/other_events_log.csv.gz"
echo "Copying it to ($s3_latest_dataset) if not already done."
aws s3 cp $s3_dataset $s3_latest_dataset --profile $1

# copy data from wordcount to S3
local_folder="data/wordcount_example/input"
s3_folder="$s3_bucket/yaetos/wordcount_example/input"
s3_dataset="$s3_folder/sample_text.txt"
echo "Putting dataset ($local_dataset) in S3 ($s3_dataset) if not already done."
aws s3 sync $local_folder $s3_folder --exclude '*' --include 'sample_text.txt' --profile $1
