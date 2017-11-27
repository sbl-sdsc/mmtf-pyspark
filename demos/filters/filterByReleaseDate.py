#!/usr/bin/env python
'''
filterByReleaseDate.py:

This example demonstrates how to filter structures with
specified releaseDate range

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import MmtfReader
from mmtfPyspark.filters import releaseDate

def main():
	path = "/home/marshuang80/PDB/reduced"

	conf = SparkConf().setMaster("local[*]") \
                      .setAppName("FilterByreleaseDate")
	sc = SparkContext(conf = conf)

	count = MmtfReader.readSequenceFile(path, sc) \
                      .filter(releaseDate("2000-01-28","2017-02-28")) \
                      .count()

	print("Number of structure released between 2000-01-28 and 2017-02-28 is : " +
		str(count))

	sc.stop()

if __name__ == "__main__":
	main()
