#!/bin/bash
# Script to configure master node on EMR cluster. (as an emr "step", not boostrap)

# Parse arguments
s3_bucket="$1"
s3_bucket_scripts="$s3_bucket/scripts.tar.gz"
echo "--- S3 path to grab files: ", $s3_bucket

# Function to print install info about libraries, to double check installs
print_lib_install() {
    local FILENAME="$1"
    # Read the file line by line
    while IFS= read -r name
    do
        # Skip empty lines and lines starting with spaces
        if [[ -z "$name" || "$name" =~ ^[[:space:]] || "$name" =~ ^# ]]; then
            continue
        fi

        # Print each name
        name_lib="${name%%==*}"
        name_lib_and_version="${name%%\#*}"
        echo "--- Checking lib $name_lib from line $name ---"
        
        # if [ "$is_executable" = true ]; then
        #     # Execute some code
        #     echo "The condition is true, executing code..."
        #     # Your code goes here
        # fi
        
        if sudo pip3 show "$name_lib" > /dev/null; then
            echo "Package '$name_lib' is installed."
            sudo pip3 show -v $name_lib
        else
            echo "Package '$name_lib' is not installed. Installing it."
            sudo pip3 install -vv --ignore-installed "$name_lib_and_version"
            echo "Package '$name_lib' forced installed. Trying it again."
            sudo pip3 show -v "$name_lib"
        fi
    done < "$FILENAME"
}

# Copy compressed script tar file from S3 to EMR master, after deploy.py moved it from laptop to S3.
echo "--- Copy S3 to EMR master ---"
aws s3 cp $s3_bucket_scripts /home/hadoop/scripts.tar.gz  # TODO check step worked or exit with failure, instead of failing silently.
aws s3 cp "$s3_bucket/setup_master.sh" /home/hadoop/setup_master.sh  # Added for debugging purposes only
aws s3 cp "$s3_bucket/setup_nodes.sh" /home/hadoop/setup_nodes.sh  # Added for debugging purposes only
aws s3 cp "$s3_bucket/requirements.txt" /home/hadoop/requirements.txt
aws s3 cp "$s3_bucket/requirements_extra.txt" /home/hadoop/requirements_extra.txt
aws s3 cp "$s3_bucket/terminate_idle_cluster.sh" /home/hadoop/terminate_idle_cluster.sh

# Install pip libs.
echo "--- Updating pip ---"
sudo pip3 install --upgrade pip
echo "--- Checking versions pre install ---"
cd /home/hadoop/
print_lib_install "requirements.txt"
print_lib_install "requirements_extra.txt"
echo "--- Installing libs ---"
echo "--- Installing requirements.txt ---"
sudo pip3 install -r requirements.txt
# Note: saw issues with libs from requirement not installed (because of other version already available).
# Can be identified with print_lib_install calls pre and post installs.
# May need to force them using "sudo pip3 install --ignore-installed somelib==x.x.x". TODO: double check.
# sudo pip3 install --ignore-installed cloudpathlib==0.16.0
echo "--- Installing requirements_extra.txt ---"
sudo pip3 install -r requirements_extra.txt
#sudo pip3 install -r --ignore-installed requirements_extra.txt
# sudo pip3 install --ignore-installed transformers==4.30.2
# sudo pip3 install --ignore-installed tensorflow==2.11.0 # latest 2.15.0
# sudo pip3 install --ignore-installed sentencepiece==0.1.99

echo "--- Checking versions post install ---"
print_lib_install "requirements.txt"
print_lib_install "requirements_extra.txt"


# Untar file
echo "--- Untaring job files ---"
mkdir -p app
tar zxvf "/home/hadoop/scripts.tar.gz" -C /home/hadoop/app/

# Creating a zip to be used as package by pyspark
# TODO: could get zip file from the start instead of tar, untar and zip.
echo "--- Zipping job files ---"
cd /home/hadoop/app
zip -r scripts.zip .

# export PYSPARK_AWS_ETL_HOME=`pwd` # TODO: enable later to be avoid hardcoded path in etl_utils.py
# . setup_oracle.sh  # uncomment if needed.

python --version # shows in stderr, ->2.7.18 on emr-5.26.0, 2.7.16 on emr-6.0.0, 2.7.18 on emr-6.1.1
python3 --version # shows in stdout, ->3.6.10 on emr-5.26.0, 3.7.4 on emr-6.0.0, 3.7.10 on emr-6.1.1
echo "Done with setup_master.sh"
