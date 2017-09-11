#!/usr/bin/env python
'''
FilterByPolymerChainType.py:

This example demonstrates how to filter the PDB by polymer chain type.

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from src.main.io import MmtfReader
from src.main.filters import *


def main():
	path = "/home/marshuang80/PDB/reduced"

	conf = SparkConf().setMaster("local[*]") \
                      .setAppName("FilterByPolymerChainType")
	sc = SparkContext(conf=conf)

	count = MmtfReader.readSequenceFile(path, sc) \
                      .filter(containsPolymerChainType(containsPolymerChainType.DNA_LINKING, containsPolymerChainType.RNA_LINKING)) \
                      .filter(notFilter(containsLProteinChain())) \
                      .filter(notFilter(containsDSaccharideChain()))
                      .count()

    print("Number of pure DNA and RNA entires: " + str(count))

    sc.stop()

if __name__ == "__main__":
	main()
