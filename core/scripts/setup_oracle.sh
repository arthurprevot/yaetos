#!/bin/bash
# Script to configure oracle. Meant to be called by setup_master if necessary

sudo yum install libaio -y
if [ ! -d /home/hadoop/conda ]; then
    PKG_URL=https://repo.continuum.io/miniconda/Miniconda2-4.5.12-Linux-x86_64.sh  # 4.5.4 works. later versions not tested.
    INSTALLER=miniconda.sh
    set -ex \
        && curl -kfSL $PKG_URL -o $INSTALLER \
        && chmod 755 $INSTALLER \
        && ./$INSTALLER -b -p /home/hadoop/conda \
        && rm $INSTALLER
        # can use ./$INSTALLER * -u * to force reinstall (implies remove check if dir exist)
fi

# didn't find a way to install oracle-instantclient from apt-get or yum
# so getting it from conda. Need to only put conda in PATH (/usr/local/bin/)
# to avoid python from conda taking over original python.
/home/hadoop/conda/bin/conda install -y -c trent oracle-instantclient=11.2.0.4.0  # full version is 11.2.0.4.0-1 but fails when adding the "-1". was "-c anaconda" until 24 oct 2019 and was pulling v11.2.0.4.0-0

sudo pip install cx_Oracle==7.2.3
ORACLE_HOME=/home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1
LD_LIBRARY_PATH=/home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib

sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libclntsh.so /usr/lib/
sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libnnz11.so /usr/lib/
sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libociei.so /usr/lib/
sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libclntsh.so.11.1 /usr/lib/
# only needed for debug: sudo ln -s -f /home/hadoop/conda/pkgs/oracle-instantclient-11.2.0.4.0-1/lib/libsqlplus.so /usr/lib/
sudo ldconfig  # To confirm libs findable : "ldd $ORACLE_HOME/bin/sqlplus"
echo "Done with Oracle setup"
