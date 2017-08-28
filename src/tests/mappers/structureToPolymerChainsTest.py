#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from src.main.mappers import structureToPolymerChains
from src.main.MmtfReader import downloadMmtfFiles
from src.main.filters import containsAlternativeLocations
#from src.main.mappers.structureToPolymerChains import *


class structureToPolymerChainsTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testContainsAlternativeLocations')
        self.sc = SparkContext(conf=conf)

        # 1STP: 1 L-protein chain:
        # 4HHB: 4 polymer chains
        # 1JLP: 1 L-protein chains with non-polymer capping group (NH2)
        # 5X6H: 1 L-protein and 1 DNA chain
        # 5L2G: 2 DNA chain
        # 2MK1: 0 polymer chains
        # --------------------
        # tot: 10 chains
        pdbIds = ["1STP","4HHB","1JLP","5X6H","5L2G","2MK1"]
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.flatMap(structureToPolymerChains())
        results_1 = pdb_1.keys().collect()

        self.assertTrue(len(results_1) == 10)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
