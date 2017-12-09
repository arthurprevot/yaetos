# setup dependencies
syspip install -r requirements.txt

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
s3_bucket="s3://bucket-scratch"
#aws s3 ...create s3 bucket if doesn't already exists
s3_folder="$s3_bucket/example_input"
s3_dataset="$s3_folder/events_log.csv.gz"
#aws s3 cp $local_dataset $s3_dataset --profile persoAP  # copy every time
aws s3 sync $local_folder $s3_folder --exclude '*' --include 'events_log.csv.gz' --profile $1
