#!/usr/bin/env python
'''
readSequenceFromListTest.py: Example reading a list of PDB IDs from a local MMTF Hadoop sequence \
file into a tubleRDD.

Authorship information:
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com:
__status__ = "Warning"
'''

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io import mmtfReader


class ReadSequenceFileTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("read_sequence_file") \
                                 .getOrCreate()

    def test_mmtf(self):
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        self.assertEqual(4, pdb.count())

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
