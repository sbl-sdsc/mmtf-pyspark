#!/usr/bin/env python
'''
Simple example of reading an MMTF Hadoop Sequence file, filtering the entries \
by rWork,and counting the number of entries.

Authorship information:
__author__ = "Peter Rose"
__maintainer__ = "Mars Huang"
__email__ = "marshuang80@gmai.com:
__status__ = "Warning"
'''

import unittest
from pyspark import SparkConf, SparkContext
from src.main.MmtfReader import downloadMmtfFiles
from src.main.filters import rWork

path = '../full'

class testContainsAlternativeLocations(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testContainsAlternativeLocations')
        pdbIds = ['4QXX','2ONX']
        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(rWork(0.171, 0.173))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('2ONX' in results_1)
        self.assertFalse('2OLX' in results_1)
        self.assertFalse('3REC' in results_1)
        self.assertFalse('1LU3' in results_1)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
