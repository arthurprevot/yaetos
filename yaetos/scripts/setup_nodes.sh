#!/bin/bash
# Script to configure all nodes on EMR cluster. (Setup as part of a boostrap operation in AWS)

# Installs below are specific to python version, which is specific to EMR version. TODO: make it independant. Will be fixed when using to emr-6.x since python3 will be default and it will support pip3.

sudo pip3 install --ignore-installed awscli==1.32.14  # AWS emr7.0.0 has version 2.14.5 installed, but 1.32.14 is latest on pypi !
sudo pip3 install boto3==1.34.14
sudo pip3 install networkx==3.1
# sudo pip3 install numpy==1.18.5  # need to force this version instead of latest (1.19.2) to be compatible with koalas 1.3.0 (requiring <1.19)
sudo pip3 install pandas==2.0.3
sudo pip3 install sqlalchemy==1.4.51  # don't use 1.3.17 since not compatible with sqlalchemy-redshift (0.7.7). See if it is still a pb at https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/issues/195
# TODO: reuse "requirement.txt" libs but just the part that need to run on the cluster side
