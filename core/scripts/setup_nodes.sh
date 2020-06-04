#!/bin/bash
# Script to configure all nodes on EMR cluster. (Setup as part of a boostrap operation in AWS)

# TODO: reuse "requirement.txt" libs but just the part that need to run on the cluster side
sudo pip3 install boto3==1.9.57
sudo pip3 install networkx==1.11
sudo pip3 install pandas==0.20.3
sudo pip3 install sqlalchemy==1.1.13
