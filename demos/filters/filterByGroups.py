#!/usr/bin/env python
'''filterByGroups.py

This example demonstrates how to filter structures with
specified groups (residues).
Groups are specified by their one, two or three letter codes
e.g. "F", "MG", "ATP", as defined in the

<a href="https://www.wwpdb.org/data/ccd">wwPDB Chemical Component Dictionary</a>.

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ContainsGroup


def main():
    path = "../../resources/mmtf_reduced_sample/""

    conf = SparkConf().setMaster("local[*]") \
        .setAppName("FilterByGroup")
    sc = SparkContext(conf=conf)

    count = mmtfReader.read_sequence_file(path, sc) \
        .filter(ContainsGroup("ATP", "MG")) \
        .count()

    print("Number of structure with ATP + MG : " +
          str(count))

    sc.stop()


if __name__ == "__main__":
    main()
