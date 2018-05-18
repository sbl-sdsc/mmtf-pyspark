#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.filters import ContainsDProteinChain
from mmtfPyspark.mappers import *


class ContainsDProteinChainTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("containsDProteinChainTest") \
                                 .getOrCreate()
        
        pdbIds = ['2ONX', '1JLP', '5X6H', '5L2G',
                  '2MK1', '2V5W', '5XDP', '5GOD']
        self.pdb = download_mmtf_files(pdbIds)

    def test1(self):
        pdb_1 = self.pdb.filter(ContainsDProteinChain())
        results_1 = pdb_1.keys().collect()
        self.assertFalse('2ONX' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertFalse('5X6H' in results_1)
        self.assertFalse('5L2G' in results_1)
        self.assertFalse('2MK1' in results_1)
        self.assertTrue('2V5W' in results_1)
        self.assertTrue('5XDP' in results_1)
        self.assertTrue('5GOD' in results_1)

    def test2(self):
        pdb_2 = self.pdb.filter(ContainsDProteinChain(exclusive=True))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertFalse('1JLP' in results_2)
        self.assertFalse('5X6H' in results_2)
        self.assertFalse('5L2G' in results_2)
        self.assertFalse('2MK1' in results_2)
        self.assertFalse('2V5W' in results_2)
        self.assertFalse('5XDP' in results_2)
        self.assertFalse('5GOD' in results_2)

    def test3(self):
        pdb_3 = self.pdb.flatMap(StructureToPolymerChains())
        pdb_3 = pdb_3.filter(ContainsDProteinChain())
        results_3 = pdb_3.keys().collect()

        self.assertFalse('2ONX.A' in results_3)
        self.assertFalse('1JLP.A' in results_3)
        self.assertFalse('5X6H.B' in results_3)
        self.assertFalse('5L2G.A' in results_3)
        self.assertFalse('5L2G.B' in results_3)
        self.assertFalse('2MK1.A' in results_3)
        self.assertFalse('5XDP.A' in results_3)
        self.assertTrue('5XDP.B' in results_3)
        self.assertFalse('2V5W.A' in results_3)
        self.assertFalse('2V5W.B' in results_3)
        self.assertTrue('2V5W.G' in results_3)
        self.assertFalse('5GOD.A' in results_3)
        self.assertFalse('5GOD.B' in results_3)
        self.assertTrue('5GOD.C' in results_3)
        self.assertTrue('5GOD.D' in results_3)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
