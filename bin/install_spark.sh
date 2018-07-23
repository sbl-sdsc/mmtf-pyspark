#!/bin/bash
# define hadoop and spark version
SPARK_VERSION='2.3.1'
HADOOP_VERSION='2.7'
OUTPUT=''

# Taking in directory specified for installation
while test $# -gt 0; do
        case "$1" in
                -h|--help)
                        echo " "
                        echo "options:"
                        echo "-h, --help                show brief help"
                        echo "-o, --output-dir=DIR      specify a directory for installation"
                        exit 0
                        ;;
                -o)
                        shift
                        if test $# -gt 0; then
                                export OUTPUT=$1
                        fi
                        shift
                        ;;
                *)
                        break
                        ;;
        esac
done

if [[ $OUTPUT = '' ]]; then
  OUTPUT=$HOME
  echo "Installation directory not specified, using default home directory: $OUTPUT"
fi 

if [ ! -d "$OUTPUT" ]; then
  OUTPUT=$HOME
  echo 'Directory does not exist, using default home directory: ' $OUTPUT 
fi

# download spark from mirror image
curl http://mirror.cogentco.com/pub/apache/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output $OUTPUT/spark.tgz

# unzip spark
cd $OUTPUT && tar -xvzf $OUTPUT/spark.tgz

# set spark home and pyspark paths
echo "export SPARK_HOME=$OUTPUT/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION" >> ~/.bashrc
echo "export PATH=$SPARK_HOME/bin:$PATH" >> ~/.bashrc
echo "export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH" >> ~/.bashrc

#cd ~
source ~/.bashrc
