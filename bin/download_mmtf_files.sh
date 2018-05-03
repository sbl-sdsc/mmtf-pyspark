#!/bin/bash
cd /tmp

# download mmtf full and reduced file
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
tar -xvf reduced.tar

# Set environmental variables
echo "export MMTF_REDUCED=/tmp/reduced/" >> ~/.bashrc
echo "export MMTF_FULL=/tmp/full/" >> ~/.bashrc

cd ~
source ~/.bashrc
