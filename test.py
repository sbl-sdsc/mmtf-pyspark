#!/usr/bin/env python
'''
test.py: Testing mmtf_spark

Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
TODO:
    Change to main funciton instead of test.py
'''
from pyspark import SparkConf, SparkContext
from MmtfSequenceFileReader import read
from filters import rFree
from filters import notFilter
from filters import *
import getopt
import sys

# Create variables
APP_NAME = "MMTF_Spark"
path = "~/PDB/full"
text = "org.apache.hadoop.io.Text"
byteWritable = "org.apache.hadoop.io.BytesWritable"

def main(argv):

    #Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #Get command line input
    path = None
    try :
        opts,args = getopt.getopt(argv,"p:",["--path="])
    except getopt.GetoptError:
        print("test.py -p <path_to_mmtf>")
        sys.exit()
    for opt,arg in opts:
        if opt in ["-p","--path"]:
            path = arg

    #Mmtf sequence file reader
    pdb = read(path,sc)

    # for testing
    print("---------------------")
    #print(pdb.filter(Rworkfilter))
    print(pdb.filter(containsDnaChain).collect())
    #print(pdb = Rworkfilter(pdb,0,0.2))
    print("----------------------")

    return pdb

if __name__ == "__main__":
    #Execute Main functionality
    main(sys.argv[1:])

