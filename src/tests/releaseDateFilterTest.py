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
from src.main.filters import releaseDate

class testReleaseDateFilter(unittest.TestCase):

    def setUp(self):

        conf = SparkConf().setMaster("local[*]").setAppName('testrFreeFilter')
        pdbIds = ['1O6Y','4MYA','3VCO','5N0Y']

        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds, self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(releaseDate("2000-01-01","2010-01-01"))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1O6Y' in results_1)
        self.assertFalse('4MYA' in results_1)
        self.assertFalse('3VCO' in results_1)
        self.assertFalse('5N0Y' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(releaseDate("2010-01-01","2020-01-01"))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('1O6Y' in results_2)
        self.assertTrue('4MYA' in results_2)
        self.assertTrue('3VCO' in results_2)
        self.assertTrue('5N0Y' in results_2)


    def test3(self):
        pdb_3 = self.pdb.filter(releaseDate("2013-03-06","2013-03-06"))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('1O6Y' in results_3)
        self.assertFalse('4MYA' in results_3)
        self.assertTrue('3VCO' in results_3)
        self.assertFalse('5N0Y' in results_3)


    def test4(self):
        pdb_4 = self.pdb.filter(releaseDate("2017-05-24","2017-05-24"))
        results_4 = pdb_4.keys().collect()

        self.assertFalse('1O6Y' in results_4)
        self.assertFalse('4MYA' in results_4)
        self.assertFalse('3VCO' in results_4)
        self.assertTrue('5N0Y' in results_4)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
