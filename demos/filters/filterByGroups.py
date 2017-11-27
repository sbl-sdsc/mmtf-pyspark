#!/usr/bin/env python
'''
filterByGroups.py

This example demonstrates how to filter structures with
specified groups (residues).
Groups are specified by their one, two or three letter codes
e.g. "F", "MG", "ATP", as defined in the

<a href="https://www.wwpdb.org/data/ccd">wwPDB Chemical Component Dictionary</a>.

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import MmtfReader
from mmtfPyspark.filters import containsGroup

def main():
	path = "/home/marshuang80/PDB/reduced"

	conf = SparkConf().setMaster("local[*]") \
                      .setAppName("FilterByGroup")
	sc = SparkContext(conf = conf)

	count = MmtfReader.readSequenceFile(path, sc) \
                      .filter(containsGroup("ATP", "MG")) \
                      .count()

	print("Number of structure with ATP + MG : " +
		str(count))

	sc.stop()

if __name__ == "__main__":
	main()
