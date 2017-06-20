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
from src.main.filters import containsRnaChain


class containsLRnaChainTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('containsDProteinChainTest')
        pdbIds = ['2ONX','1JLP','5X6H','5L2G','2MK1','5UX0','2NCQ']
        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(containsRnaChain())
        results_1 = pdb_1.keys().collect()
        self.assertFalse('2ONX' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertFalse('5X6H' in results_1)
        self.assertFalse('5L2G' in results_1)
        self.assertFalse('2MK1' in results_1)
        self.assertTrue('5UX0' in results_1)
        self.assertTrue('2NCQ' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(containsRnaChain(exclusive = True))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertFalse('1JLP' in results_2)
        self.assertFalse('5X6H' in results_2)
        self.assertFalse('5L2G' in results_2)
        self.assertFalse('2MK1' in results_2)
        self.assertFalse('5UX0' in results_2)
        self.assertTrue('2NCQ' in results_2)


    # TODO: Mapper structure to polymer chains
    '''
    def test3(self):
        pdb_3 = self.pdb.filter(rWork(0.10, 0.16))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('2ONX' in results_3)
        self.assertFalse('2OLX' in results_3)
        self.assertFalse('3REC' in results_3)
        self.assertFalse('1LU3' in results_3)
    '''

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
