#!/usr/bin/env python
'''
filterByRFree.py:

Example of reading an MMTF Hadoop Sequence file,
filtering the entries by rFree, and counting the
number of entires. This example shows how methods can
be chained together

@see<a href="http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/r-value-and-r-free">rfree</a>

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import MmtfReader
from mmtfPyspark.filters import rFree

def main():
    path = "../../resources/mmtf_reduced_sample/"

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("filterByResolution")
    sc = SparkContext(conf = conf)

    count = MmtfReader.read_sequence_file(path, sc) \
                      .filter(rFree(0.0,2.0)) \
                      .count()

    print("Number of structures : " +str(count))

    sc.stop()

if __name__ == "__main__":
    main()
