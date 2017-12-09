# setup dependencies
pip install -r requirements.txt

# create data locally
mkdir -p data/
mkdir -p data/example_input
local_folder="data/example_input"
local_dataset="$local_folder/events_log.csv.gz"
if [ ! -f $local_dataset ]; then
    echo "Downloading: " + $local_dataset
    curl https://raw.githubusercontent.com/wikimedia-research/Discovery-Hiring-Analyst-2016/master/events_log.csv.gz -o $local_dataset
fi


# create data on s3
bucket_name="bucket-scratch"

if ! aws s3api head-bucket --bucket $bucket_name --profile $1 2>/dev/null; then
  echo "S3 Bucket '$bucket_name' doesn't exist. Please create it in AWS and rerun this script."
  exit 1
fi

s3_bucket="s3://$bucket_name"
s3_folder="$s3_bucket/example_input"
s3_dataset="$s3_folder/events_log.csv.gz"
echo "Putting dataset ($local_dataset) in S3 ($s3_dataset) if not already done."
aws s3 sync $local_folder $s3_folder --exclude '*' --include 'events_log.csv.gz' --profile $1
