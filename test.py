#!/usr/bin/env python
'''
test.py: Testing mmtf_spark

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
'''

import sys
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import *
from mmtfPyspark.filters import resolution
import getopt


def main(argv):
    # Create variables
    APP_NAME = "MMTF_Spark"
    path = "<Path to your MMTF Files?"

    #Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]").set("spark.ui.port", "4040" ).set("spark.driver.maxResultSize","16g")
    sc = SparkContext(conf=conf)


    #Get command line input
    try :
        opts,args = getopt.getopt(argv,"p:",["--path="])
    except getopt.GetoptError:
        print("test.py -p <path_to_mmtf>")
        sys.exit()

    for opt,arg in opts:
        if opt in ["-p","--path"]:
            path = arg

    #Mmtf sequence file reader
    #pdbIds = ['5GOD','1B38','1B39','1BUH','1C25','1CKP','1DI8','1DM2','1E1V']
    #pdb = downloadMmtfFiles(pdbIds,sc)

    pdb = readSequenceFile(path,sc)
    print(f"Total number of protein entries in PDB : {pdb.count()}")

    #pdb = readSequenceFile(path,sc,fraction = 0.1, seed = 7)

    sc.stop()

    return pdb

if __name__ == "__main__":

    #Execute Main functionality
    main(sys.argv[1:])
