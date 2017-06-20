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
from src.main.filters import containsGroup


path = '../full'

class testContainsDnaChainTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testContainsAlternativeLocations')
        pdbIds = ['1STP','1JLP','5X6H','5L2G','2MK1']
        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(containsGroup('BTN'))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1STP' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertFalse('5X6H' in results_1)
        self.assertFalse('5L2G' in results_1)
        self.assertFalse('2MK1' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(containsGroup('HYP'))
        results_2 = pdb_2.keys().collect()

        self.assertFalse("1STP" in results_2);
        self.assertTrue("1JLP" in results_2);
        self.assertFalse("5X6H" in results_2);
        self.assertFalse("5L2G" in results_2);
        self.assertFalse("2MK1" in results_2);


    def test3(self):
        pdb_3 = self.pdb.filter(containsGroup('HYP','NH2'))
        results_3 = pdb_3.keys().collect()

        self.assertFalse("1STP" in results_3);
        self.assertTrue("1JLP" in results_3);
        self.assertFalse("5X6H" in results_3);
        self.assertFalse("5L2G" in results_3);
        self.assertFalse("2MK1" in results_3);


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
