#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import downloadMmtfFiles
from mmtfPyspark.rcsbfilters import pisces
from mmtfPyspark.mappers import *

class piscesTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('piscesTest')
        self.sc = SparkContext(conf=conf)

        # "4R4X.A" and "5X42.B" should pass filter
        pdbIds = ["5X42","4R4X","2ONX","1JLP"]
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(pisces(20,2.0))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('5X42' in results_1)
        self.assertTrue('4R4X' in results_1)
        self.assertFalse('2ONX' in results_1)
        self.assertFalse('1JLP' in results_1)


    def test2(self):
        pdb_2 = self.pdb.flatMap(structureToPolymerChains())
        pdb_2 = pdb_2.filter(pisces(20,2.0))
        results_2 = pdb_2.keys().collect()

        self.assertTrue('5X42.B' in results_2)
        self.assertTrue('4R4X.A' in results_2)
        self.assertFalse('5X42.A' in results_2)
        self.assertFalse('2ONX.A' in results_2)
        self.assertFalse('1JLP.A' in results_2)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
