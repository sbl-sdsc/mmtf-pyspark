#!/usr/bin/env python
'''filterByResolution.py:

Example of reading an MMTF Hadoop Sequence file,
filtering the entires by resolution,
and counting the number of entires

@see<a href="http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/resolution">resolution</a>

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import Resolution
import time


def main():
    path = "../../resources/mmtf_reduced_sample/"
    start = time.time()

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("filterByResolution")
    sc = SparkContext(conf=conf)

    count = mmtfReader.read_sequence_file(path, sc) \
                      .filter(Resolution(0.0, 2.0)) \
                      .count()

    print("Number of structures : " + str(count))

    sc.stop()

    end = time.time()
    print(str(end - start) + " sec.")


if __name__ == "__main__":
    main()
