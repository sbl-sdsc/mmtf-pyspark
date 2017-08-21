#!/usr/bin/env python
'''
Simple example of reading an MMTF Hadoop Sequence file, filtering the entries \
by rFree,and counting the number of entries.

Authorship information:
__author__ = "Peter Rose"
__maintainer__ = "Mars Huang"
__email__ = "marshuang80@gmai.com:
__status__ = "Warning"
'''
# TODO Traceback "ResourceWarning: unclosed filecodeDecodeError: 'ascii' codec can't decode byte 0xc3 in position 25: ordinal not in range(128)"
# TODO No actual value for unit test

import unittest
from pyspark import SparkConf, SparkContext
from src.main.MmtfReader import downloadMmtfFiles
from src.main.filters import secondaryStructure

class secondaryStructureTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testrFreeFilter')
        pdbIds = ["1AIE","1E0N","1EM7","2C7M"]
        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds, self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(secondaryStructure(0.64, 0.65, 0.0, 0.0, 0.35, 0.36))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1AIE' in results_1)
        self.assertFalse('1E0N' in results_1)
        self.assertFalse('1EM7' in results_1)
        self.assertFalse('2CTM' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(secondaryStructure(0.0, 0.0, 0.48, 0.49, 0.51, 0.52))
        results_2 = pdb_2.keys().collect()

        self.assertTrue('1E0N' in results_2)


    def test3(self):
        pdb_3 = self.pdb.filter(secondaryStructure(0.24, 0.26, 0.41, 0.42, 0.33, 0.34))
        results_3 = pdb_3.keys().collect()

        self.assertTrue('1EM7' in results_3)


    def test4(self):
        pdb_4 = self.pdb.filter(secondaryStructure(0.70, 0.80, 0.00, 0.20, 0.20, 0.30, \
                                                   exclusive = True))
        results_4 = pdb_4.keys().collect()

        self.assertFalse('2C7M' in results_4)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
