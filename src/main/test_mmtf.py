#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from MmtfReader import downloadMmtfFiles
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
proteins = ['2ONX','1JLP','5X6H','5L2G','2MK1']


pdb = downloadMmtfFiles(proteins,sc)
# for testing
print("---------------------")
#print(pdb.filter(Rworkfilter))
pdb = pdb.collect()
#print(pdb = Rworkfilter(pdb,0,0.2))
print("----------------------")
t = pdb[0][1]
