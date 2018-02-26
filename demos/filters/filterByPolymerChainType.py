#!/usr/bin/env python
'''FilterByPolymerChainType.py:

This example demonstrates how to filter the PDB by polymer chain type.

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import MmtfReader
from mmtfPyspark.filters import *


def main():
	path = "../../resources/mmtf_reduced_sample/"

	conf = SparkConf().setMaster("local[*]") \
                      .setAppName("FilterByPolymerChainType")
	sc = SparkContext(conf=conf)

	count = MmtfReader.read_sequence_file(path, sc) \
                      .filter(ContainsPolymerChainType(ContainsPolymerChainType.DNA_LINKING, ContainsPolymerChainType.RNA_LINKING)) \
                      .filter(NotFilter(ContainsLProteinChain())) \
                      .filter(NotFilter(ContainsDSaccharideChain()))
                      .count()

    print("Number of pure DNA and RNA entires: " + str(count))

    sc.stop()

if __name__ == "__main__":
	main()
