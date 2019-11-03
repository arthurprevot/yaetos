#!/bin/bash
# Script to configure all nodes on EMR cluster.

# TODO: reuse "requirement.txt" libs but just the part that need to run on the cluster side
sudo pip install boto3==1.9.57
sudo pip install networkx==1.11
sudo pip install pandas==0.20.3
sudo pip install sqlalchemy==1.1.13
