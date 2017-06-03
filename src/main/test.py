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
from MmtfReader import downloadMmtfFiles, readSequenceFile
from filters import rFree
from filters import notFilter
from filters import resolution
import getopt
import sys

# Create variables
APP_NAME = "MMTF_Spark"
path = "~/PDB/reduced"
# text = "org.apache.hadoop.io.Text"
# byteWritable = "org.apache.hadoop.io.BytesWritable"

def main(argv):

    #Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    conf = conf.set("spark.executor.memory", "64g")
    conf = conf.set("spark.driver.cores","32")
    sc = SparkContext(conf=conf)

    #Get command line input
    path = "../reduced"
    try :
        opts,args = getopt.getopt(argv,"p:",["--path="])
    except getopt.GetoptError:
        print("test.py -p <path_to_mmtf>")
        sys.exit()
    for opt,arg in opts:
        if opt in ["-p","--path"]:
            path = arg

    #Mmtf sequence file reader
    #pdbIds = ['1AQ1','1B38','1B39','1BUH','1C25','1CKP','1DI8','1DM2','1E1V'\
    #,'1E1X','1E9H','1F5Q','1FIN','1FPZ','1FQ1','1FQV','1FS1']
    #pdb = downloadMmtfFiles(pdbIds,sc)
    #print(pdb.filter(lambda t: t[0]).collect())

    pdb = readSequenceFile(path,sc)

    #pdb = readSequenceFile(path,sc,fraction = 0.5, seed = 7)
    #pdb =pdb.filter(lambda t: t[0]).collect()
    #print(pdb[:5])
    # for testing
    print("---------------------")
    pdb.filter(resolution(0.0,0.2)).count()
    #print(pdb.filter(containsDnaChain).collect())
    #print(pdb = Rworkfilter(pdb,0,0.2))
    print("----------------------")

    return pdb

if __name__ == "__main__":
    #Execute Main functionality
    main(sys.argv[1:])

