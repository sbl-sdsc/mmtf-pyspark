#!/usr/bin/env python
'''
UniProtDemo.py:

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from src.main.datasets import UniProt
from src.main.datasets.UniProt import UniProtDataset
import time


def main():
    start = time.time()

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("UniProtDemo")
    sc = SparkContext(conf = conf)

    # Read Jpred Dataset
    ds = UniProt.getDataset(UniProtDataset.SWISS_PROT)
    ds.show(20, False)

    end = time.time()

    print("Time: %f  sec." %(end-start))

    sc.stop()

if __name__ == "__main__":
    main()
