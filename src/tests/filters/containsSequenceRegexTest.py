#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from src.main.MmtfReader import downloadMmtfFiles
from src.main.filters import containsSequenceRegex
from src.main.mappers.structureToPolymerChains import *

class containsSequenceRegexTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('containsDProteinChainTest')
        self.sc = SparkContext(conf=conf)

        # 5KE8: contains Zinc finger motif
        # 1JLP: does not contain Zinc finger motif
        # 5VAI: contains Walker P loop
        pdbIds = ['5KE8','1JLP','5VAI']
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(containsSequenceRegex("C.{2,4}C.{12}H.{3,5}H"))
        results_1 = pdb_1.keys().collect()
        self.assertTrue('5KE8' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertFalse('5VAI' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(containsSequenceRegex("[AG].{4}GK[ST]"))
        results_2 = pdb_2.keys().collect()
        self.assertFalse('5KE8' in results_2)
        self.assertFalse('1JLP' in results_2)
        self.assertTrue('5VAI' in results_2)


    def test3(self):
        pdb_3 = self.pdb.flatMap(structureToPolymerChains())
        pdb_3 = pdb_3.filter(containsSequenceRegex("C.{2,4}C.{12}H.{3,5}H"))
        results_3 = pdb_3.keys().collect()

        self.assertTrue('5KE8.A' in results_3)
        self.assertFalse('5KE8.B' in results_3)
        self.assertFalse('5KE8.C' in results_3)
        self.assertFalse('1JLC.A' in results_3)
        self.assertFalse('5VAI.A' in results_3)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
