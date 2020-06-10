#!/bin/bash
# Script to configure all nodes on EMR cluster. (Setup as part of a boostrap operation in AWS)

# TODO: reuse "requirement.txt" libs but just the part that need to run on the cluster side
# Installs below are specific to python version, which is specific to EMR version. TODO: make it independant. Will be fixed when using to emr-6.x since python3 will be default and it will support pip3.
sudo pip-3.6 install boto3==1.9.57
sudo pip-3.6 install networkx==1.11
sudo pip-3.6 install pandas==1.0.4
sudo pip-3.6 install sqlalchemy==1.3.15  # don't use 1.3.17 since not compatible with sqlalchemy-redshift. See if it is still a pb at https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/issues/195
