#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from src.main.MmtfReader import downloadMmtfFiles
from src.main.filters import containsDnaChain
# from src.main.mappers.structureToPolymerChains import structureToPolymerChains
from src.main.mappers import *

class testContainsDnaChainTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testContainsAlternativeLocations')
        self.sc = SparkContext(conf=conf)

        # 2ONX: only L-protein chain
        # 1JLP: single L-protein chains with non-polymer capping group (NH2)
        # 5X6H: L-protein and non-std. DNA chain
        # 5L2G: DNA chain
        # 2MK1: D-saccharide
        pdbIds = ['2ONX','1JLP','5X6H','5L2G','2MK1']
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(containsDnaChain())
        results_1 = pdb_1.keys().collect()

        self.assertFalse('2ONX' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertTrue('5X6H' in results_1)
        self.assertTrue('5L2G' in results_1)
        self.assertFalse('2MK1' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(containsDnaChain(exclusive = True))
        results_2 = pdb_2.keys().collect()

        self.assertFalse("2ONX" in results_2);
        self.assertFalse("1JLP" in results_2);
        self.assertFalse("5X6H" in results_2);
        self.assertTrue("5L2G" in results_2);
        self.assertFalse("2MK1" in results_2);


    def test3(self):
        pdb_3 = self.pdb.flatMap(structureToPolymerChains.structureToPolymerChains())
        pdb_3 = pdb_3.filter(containsDnaChain())
        results_3 = pdb_3.keys().collect()

        self.assertFalse("2ONX.A" in results_3);
        self.assertFalse("1JLP.A" in results_3);
        self.assertFalse("5X6H.A" in results_3);
        self.assertFalse("5X6H.B" in results_3);
        self.assertTrue("5L2G.A" in results_3);
        self.assertTrue("5L2G.B" in results_3);
        self.assertFalse("2MK1.A" in results_3);


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
