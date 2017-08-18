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
from src.main.filters import polymerComposition


class polymerCompositionTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testContainsAlternativeLocations')
        pdbIds = ["2ONX","1JLP","5X6H","5L2G","2MK1","5UZT","1AA6","1NTH"]
        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(polymerComposition(polymerComposition.AMINO_ACIDS_20))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('2ONX' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertTrue('5X6H' in results_1)
        self.assertFalse('5L2G' in results_1)
        self.assertFalse('2MK1' in results_1)
        self.assertFalse('5UZT' in results_1)
        self.assertFalse('1AA6' in results_1)
        self.assertFalse('1NTH' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(polymerComposition(polymerComposition.AMINO_ACIDS_20, exclusive = True))
        results_2 = pdb_2.keys().collect()

        self.assertTrue('2ONX' in results_2)
        self.assertFalse('1JLP' in results_2)
        self.assertFalse('5X6H' in results_2)
        self.assertFalse('5L2G' in results_2)
        self.assertFalse('2MK1' in results_2)
        self.assertFalse('5UZT' in results_2)
        self.assertFalse('1AA6' in results_2)
        self.assertFalse('1NTH' in results_2)


    # TODO test3 needs mapper
    # TODO test4 needs mapper


    def test5(self):
        pdb_5 = self.pdb.filter(polymerComposition(polymerComposition.DNA_STD_NUCLEOTIDES))
        results_5 = pdb_5.keys().collect()

        self.assertFalse('2ONX' in results_5)
        self.assertFalse('1JLP' in results_5)
        self.assertTrue('5X6H' in results_5)
        self.assertFalse('5L2G' in results_5)
        self.assertFalse('2MK1' in results_5)
        self.assertFalse('5UZT' in results_5)

    def test6(self):
        pdb_6 = self.pdb.filter(polymerComposition(polymerComposition.RNA_STD_NUCLEOTIDES))
        results_6 = pdb_6.keys().collect()

        self.assertFalse('2ONX' in results_6)
        self.assertFalse('1JLP' in results_6)
        self.assertFalse('5X6H' in results_6)
        self.assertFalse('5L2G' in results_6)
        self.assertFalse('2MK1' in results_6)
        self.assertTrue('5UZT' in results_6)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
