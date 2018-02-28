#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.filters import ContainsRnaChain
from mmtfPyspark.mappers import *


class ContainsLRnaChainTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('containsRNAChainTest')
        self.sc = SparkContext(conf=conf)

        # 2ONX: only L-protein chain
        # 1JLP: single L-protein chains with non-polymer capping group (NH2)
        # 5X6H: L-protein and DNA chain
        # 5L2G: DNA chain
        # 2MK1: D-saccharide
        # 5UX0: 2 L-protein, 2 RNA, 2 DNA chains
        # 2NCQ: 2 RNA chains
        pdbIds = ['2ONX', '1JLP', '5X6H', '5L2G', '2MK1', '5UX0', '2NCQ']
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        pdb_1 = self.pdb.filter(ContainsRnaChain())
        results_1 = pdb_1.keys().collect()
        self.assertFalse('2ONX' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertFalse('5X6H' in results_1)
        self.assertFalse('5L2G' in results_1)
        self.assertFalse('2MK1' in results_1)
        self.assertTrue('5UX0' in results_1)
        self.assertTrue('2NCQ' in results_1)

    def test2(self):
        pdb_2 = self.pdb.filter(ContainsRnaChain(exclusive=True))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertFalse('1JLP' in results_2)
        self.assertFalse('5X6H' in results_2)
        self.assertFalse('5L2G' in results_2)
        self.assertFalse('2MK1' in results_2)
        self.assertFalse('5UX0' in results_2)
        self.assertTrue('2NCQ' in results_2)

    def test3(self):
        pdb_3 = self.pdb.flatMap(StructureToPolymerChains())
        pdb_3 = pdb_3.filter(ContainsRnaChain())
        results_3 = pdb_3.keys().collect()

        self.assertFalse('2ONX.A' in results_3)
        self.assertFalse('1JLP.A' in results_3)
        self.assertFalse('5X6H.A' in results_3)
        self.assertFalse('5X6H.B' in results_3)
        self.assertFalse('5L2G.A' in results_3)
        self.assertFalse('5L2G.B' in results_3)
        self.assertFalse('2MK1.A' in results_3)
        self.assertFalse('5UX0.A' in results_3)
        self.assertFalse('5UX0.D' in results_3)
        self.assertTrue('5UX0.B' in results_3)
        self.assertTrue('5UX0.E' in results_3)
        self.assertFalse('5UX0.C' in results_3)
        self.assertFalse('5UX0.F' in results_3)
        self.assertTrue('2NCQ.A' in results_3)
        self.assertTrue('2NCQ.S' in results_3)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
