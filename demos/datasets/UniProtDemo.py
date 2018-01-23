#!/usr/bin/env python
'''
UniProtDemo.py:

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.datasets import UniProt
#from mmtfPyspark.datasets import UniProtDataset # TODO find ways to skip import
import time


def main():
    start = time.time()

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("UniProtDemo")
    sc = SparkContext(conf = conf)

    # Read Jpred Dataset
    #ds = UniProt.getDataset(UniProt.SWISS_PROT)
    ds = UniProt.getDataset(UniProt.UNIREF50)

    ds.show(20, False)

    end = time.time()

    print("Time: %f  sec." %(end-start))

    sc.stop()

if __name__ == "__main__":
    main()
