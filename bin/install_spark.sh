#!/bin/bash
# define hadoop and spark version
SPARK_VERSION='2.2.1'
HADOOP_VERSION='2.7'

# download spark from mirror image
curl http://mirror.cogentco.com/pub/apache/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output /tmp/spark.tgz

# unzip spark
cd /tmp && tar -xvzf /tmp/spark.tgz

# set spark home and pyspark paths
echo "SPARK_HOME=/tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION" >> ~/.bashrc
echo "export PATH=$SPARK_HOME/bin:$PATH" >> ~/.bashrc
echo "export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH" >> ~/.bashrc
#export PYSPARK_SUBMIT_ARGS="--master local[*] pyspark-shell"
