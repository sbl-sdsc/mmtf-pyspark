#!/usr/bin/env python
'''
MmtfBenchmark.py: Benchmarking of MMTF-Hadoop Sequence file reading

Authorship information:
    __author__ = "Peter Rose"
'''

import sys
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import *
import getopt
import time


def main(argv):

    start = time.time()

    #Configure Spark
    conf = SparkConf()
    sc = SparkContext(conf = conf)


    #Get command line input
    try :
        opts,args = getopt.getopt(argv,"p:",["--path="])
    except getopt.GetoptError:
        print("test.py -p <mmtf-hadoop-sequence-file>")
        sys.exit()

    for opt,arg in opts:
        if opt in ["-p","--path"]:
            path = arg

    pdb = read_sequence_file(path,sc)

    print(f"path: {path}")
    print(f"structures: {pdb.count()} cores: {sc.defaultParallelism} partitions: {pdb.getNumPartitions()}")

    sc.stop()

    end = time.time()
    print("time: " + str(end-start) + " sec.")


if __name__ == "__main__":
    main(sys.argv[1:])
