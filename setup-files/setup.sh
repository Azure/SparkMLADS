#!/usr/bin/env bash

echo "Running the action script to upgrade MRS and set up the tutorial"

# Upgrade MRS

apt-get -y remove microsoft-r-server-hadoop-9.0
apt-get -y remove microsoft-r-server-packages-9.0
apt-get -y remove microsoft-r-open-foreachiterators-3.3
apt-get -y remove microsoft-r-open-intel-mkl-3.3
apt-get -y remove microsoft-r-open-mro-3.3
wget https://mrseagle.blob.core.windows.net/share/MRS_Linux.zip -O /tmp/MRS_Linux.zip
cd /tmp
unzip MRS_Linux.zip
cd /tmp/MRS_Linux
chmod 777 install.sh
./install.sh -amp

# Set up the tutorial

cd /home/sshuser

git clone https://github.com/Azure/SparkMLADS
chown -R sshuser:sshuser SparkMLADS

mkdir /var/RevoShare/sshuser
chown sshuser:sshuser /var/RevoShare/sshuser

# Airline data
wget https://marinchpub.blob.core.windows.net/airwxpub/Air2009to2012CSV.tgz
wget https://marinchpub.blob.core.windows.net/airwxpub/Weather.tgz
tar xzf Air2009to2012CSV.tgz
tar xzf Weather.tgz

hdfs dfs -mkdir hdfs://mycluster/share
hdfs dfs -copyFromLocal Air2009to2012CSV hdfs://mycluster/share
hdfs dfs -copyFromLocal Weather hdfs://mycluster/share

Revo64 -e 'install.packages(c("sparklyr", "ggplot2", "tidyr"), repos = "https://mran.microsoft.com/snapshot/2017-05-01")'

