#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import download_mmtf_files
from mmtfPyspark.filters import OrFilter, ContainsDnaChain, ContainsRnaChain
from mmtfPyspark.mappers import *


class OrFilterTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('OrFilterTest')
        self.sc = SparkContext(conf=conf)

        # 2ONX: only L-protein chain
        # 1JLP: single L-protein chains with non-polymer capping group (NH2)
        # 5X6H: L-protein and non-std. DNA chain
        # 5L2G: DNA chain
        # 2MK1: D-saccharide
        # 5UZT: RNA chain (with std. nucleotides)
        # 1AA6: contains SEC, selenocysteine (21st amino acid)
        # 1NTH: contains PYL, pyrrolysine (22nd amino acid)
        pdbIds = ['2ONX', '1JLP', '5X6H', '5L2G',
                  '2MK1', '5UZT', '1AA6', '1NTH']
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        pdb_1 = self.pdb.flatMap(StructureToPolymerChains())
        pdb_1 = pdb_1.filter(OrFilter(ContainsDnaChain(), ContainsRnaChain()))
        results_1 = pdb_1.keys().collect()

        self.assertFalse('2ONX.A' in results_1)
        self.assertFalse('1JLP.A' in results_1)
        self.assertFalse('5X6H.A' in results_1)
        self.assertFalse('5X6H.B' in results_1)
        self.assertTrue('5L2G.A' in results_1)
        self.assertTrue('5L2G.B' in results_1)
        self.assertFalse('2MK1.A' in results_1)
        self.assertTrue('5UZT.A' in results_1)
        self.assertFalse('1AA6.A' in results_1)
        self.assertFalse('1NTH.A' in results_1)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
