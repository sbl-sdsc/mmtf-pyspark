#!/usr/bin/env python
'''
filterByDepositionDate.py:

This example demonstrates how to filter structures with
specified depositionDate range

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import MmtfReader
from mmtfPyspark.filters import depositionDate

def main():
	path = "/home/marshuang80/PDB/reduced"

	conf = SparkConf().setMaster("local[*]") \
                      .setAppName("FilterByDepositionDate")
	sc = SparkContext(conf = conf)

	count = MmtfReader.readSequenceFile(path, sc) \
                      .filter(depositionDate("2016-01-28","2017-02-28")) \
                      .count()

	print("Number of structure desposited between 2016-01-28 and 2017-02-28 is : " +
		str(count))

	sc.stop()

if __name__ == "__main__":
	main()
