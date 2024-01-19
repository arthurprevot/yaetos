#!/bin/bash
# Script to configure master node on EMR cluster. (as an emr "step", not boostrap)

# Parse arguments
s3_bucket="$1"
s3_bucket_scripts="$s3_bucket/scripts.tar.gz"
echo "--- S3 path to grab files: ", $s3_bucket

# Function to print install info about libraries, to double check installs
print_lib_install() {
    local FILENAME="$1"
    local install_if_missing="$2"

    # Read the file line by line
    while IFS= read -r lib_line
    do
        # Skip empty lines and lines starting with spaces
        if [[ -z "$lib_line" || "$lib_line" =~ ^[[:space:]] || "$lib_line" =~ ^# ]]; then
            continue
        fi

        # Print each lib_line
        name_lib="${lib_line%%==*}"
        name_lib_and_version="${lib_line%%\#*}"
        echo "--- Checking lib $name_lib from line $lib_line ---"
        
        if sudo pip3 show "$name_lib" > /dev/null; then
            echo "Package '$name_lib' is installed."
            sudo pip3 show $name_lib
        elif [ "$install_if_missing" = true ]; then
            echo "Package '$name_lib' is not installed. Installing it."
            sudo pip3 install --ignore-installed "$name_lib_and_version"
            echo "Package '$name_lib' forced installed. Trying it again."
            sudo pip3 show "$name_lib"
        else
            echo "Package '$name_lib' is not installed. installation skipped."
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
print_lib_install "requirements.txt" false
print_lib_install "requirements_extra.txt" false
echo "--- Installing libs ---"
echo "--- Installing requirements.txt ---"
sudo pip3 install -r requirements.txt
# Note: saw issues with libs from requirement not installed after this step (likely because of other version already available).
# May need to force them using "sudo pip3 install --ignore-installed somelib==x.x.x"
# Handled as part of print_lib_install calls below.
echo "--- Installing requirements_extra.txt ---"
sudo pip3 install -r requirements_extra.txt
echo "--- Checking versions post install ---"
print_lib_install "requirements.txt" true
print_lib_install "requirements_extra.txt" true


# Untar file
echo "--- Untaring job files ---"
mkdir -p app
tar zxvf "/home/hadoop/scripts.tar.gz" -C /home/hadoop/app/

# Creating a zip to be used as package by pyspark
# TODO: could get zip file from the start instead of tar, untar and zip.
echo "--- Zipping job files ---"
cd /home/hadoop/app
zip -r scripts.zip .

# . setup_oracle.sh  # uncomment if needed. May not work out of the box because of version updates.

python --version # shows in stderr, ->3.9.16 on emr-7.0.0, was 2.7.18 on emr-5.26.0, 2.7.16 on emr-6.0.0, 2.7.18 on emr-6.1.1
python3 --version # shows in stdout, ->3.9.16 on emr-7.0.0, was 3.6.10 on emr-5.26.0, 3.7.4 on emr-6.0.0, 3.7.10 on emr-6.1.1
echo "Done with setup_master.sh"
