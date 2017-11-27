#!/usr/bin/env python
'''
JpredDemo.py:


Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from src.main.datasets import JpredDataset
import time


def main():
    start = time.time()

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("JpredDemo")
    sc = SparkContext(conf = conf)

    # Read Jpred Dataset
    res = JpredDataset.getDataset()
    res.show(10)

    # Write to Json file
    res = res.coalesce(1)
    res.write.format("json").save("/home/marshuang80/backup/JpredData3")


    end = time.time()

    print("Time: %f  sec." %(end-start))

    sc.stop()

if __name__ == "__main__":
    main()


