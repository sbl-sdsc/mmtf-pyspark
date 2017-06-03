#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from MmtfReader import read
from filters import rFree
# Create variables
APP_NAME = "MMTF_Spark"
path = "../reduced"
text = "org.apache.hadoop.io.Text"
byteWritable = "org.apache.hadoop.io.BytesWritable"
#Configure Spark
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc = SparkContext(conf=conf)
#Mmtf sequence file reader
pdb = read(path,sc)
# for testing
print("---------------------")
#print(pdb.filter(Rworkfilter))
pdb = pdb.filter(rFree(0,1)).collect()
#print(pdb = Rworkfilter(pdb,0,0.2))
print("----------------------")

mmtf_test = pdb[0][1]
