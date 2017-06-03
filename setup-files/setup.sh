#!/usr/bin/env bash

echo "Running the action script to set up the tutorial"

cd /home/sshuser

git clone https://github.com/Azure/SparkMLADS

mkdir /var/RevoShare/sshuser
chown sshuser:sshuser /var/RevoShare/sshuser

# Airline data
wget https://cdspsparksamples.blob.core.windows.net/data/Airline/WeatherSubsetCsv.tar.gz
wget https://cdspsparksamples.blob.core.windows.net/data/Airline/AirlineSubsetCsv.tar.gz
tar xzf AirlineSubsetCsv.tar.gz
tar xzf WeatherSubsetCsv.tar.gz

hdfs dfs -mkdir /share
hdfs dfs -copyFromLocal AirlineSubsetCsv /share
hdfs dfs -copyFromLocal WeatherSubsetCsv /share

Revo64 -e 'install.packages("sparklyr", repos = "https://mran.microsoft.com/snapshot/2017-05-01")'

