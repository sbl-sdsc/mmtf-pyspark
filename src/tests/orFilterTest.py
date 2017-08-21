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
from src.main.filters import orFilter,containsDnaChain, containsRnaChain

# TODO Need mappers for this unit test to work
class testOrFilter(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testrFreeFilter')
        pdbIds = ['2ONX','1JLP','5X6H','5L2G','2MK1','5UZT','1AA6','1NTH']
        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds, self.sc)


    def test1(self):
        pdb = pdb.flatMapToPair(new StructureToPolymerChains())
        pdb_1 = self.pdb.filter(orFilter(containsDnaChain(),containsRnaChain()))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('2ONX.A' in results_1)
        self.assertFalse('1JLP.A' in results_1)
        self.assertFalse('5X6H.A' in results_1)
        self.assertFalse('5X6H.B' in results_1)
        self.assertFalse('5L2G.A' in results_1)
        self.assertFalse('5L2G.A' in results_1)
        self.assertFalse('5L2G.B' in results_1)
        self.assertFalse('2MK1.A' in results_1)
        self.assertFalse('5UZT.A' in results_1)
        self.assertFalse('1AA6.A' in results_1)
        self.assertFalse('1NTH.A' in results_1)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
