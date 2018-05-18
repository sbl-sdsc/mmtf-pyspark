#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.filters import PolymerComposition
from mmtfPyspark.mappers import *


class PolymerCompositionTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("PolymerCompositionTest") \
                                 .getOrCreate()

        # 2ONX: only L-protein chain
        # 1JLP: single L-protein chains with non-polymer capping group (NH2)
        # 5X6H: L-protein and DNA chain (with std. nucleotides)
        # 5L2G: DNA chain (with non-std. nucleotide)
        # 2MK1: D-saccharide
        # 5UZT: RNA chain (with std. nucleotides)
        # 1AA6: contains SEC, selenocysteine (21st amino acid)
        # 1NTH: contains PYL, pyrrolysine (22nd amino acid)
        pdbIds = ["2ONX", "1JLP", "5X6H", "5L2G",
                  "2MK1", "5UZT", "1AA6", "1NTH"]
        self.pdb = download_mmtf_files(pdbIds)

    def test1(self):
        pdb_1 = self.pdb.filter(PolymerComposition(
            PolymerComposition.AMINO_ACIDS_20))
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
        pdb_2 = self.pdb.filter(PolymerComposition(
            PolymerComposition.AMINO_ACIDS_20, exclusive=True))
        results_2 = pdb_2.keys().collect()

        self.assertTrue('2ONX' in results_2)
        self.assertFalse('1JLP' in results_2)
        self.assertFalse('5X6H' in results_2)
        self.assertFalse('5L2G' in results_2)
        self.assertFalse('2MK1' in results_2)
        self.assertFalse('5UZT' in results_2)
        self.assertFalse('1AA6' in results_2)
        self.assertFalse('1NTH' in results_2)

    def test3(self):
        pdb_3 = self.pdb.flatMap(StructureToPolymerChains())
        pdb_3 = pdb_3.filter(PolymerComposition(
            PolymerComposition.AMINO_ACIDS_20))
        results_3 = pdb_3.keys().collect()

        self.assertTrue('2ONX.A' in results_3)
        self.assertFalse('1JLP.A' in results_3)
        self.assertTrue('5X6H.B' in results_3)
        self.assertFalse('5L2G.A' in results_3)
        self.assertFalse('5L2G.B' in results_3)
        self.assertFalse('2MK1.A' in results_3)
        self.assertFalse('5UZT.A' in results_3)
        self.assertFalse('1AA6.A' in results_3)
        self.assertFalse('1NTH.A' in results_3)

    def test4(self):
        pdb_4 = self.pdb.flatMap(StructureToPolymerChains())
        pdb_4 = pdb_4.filter(PolymerComposition(
            PolymerComposition.AMINO_ACIDS_22))
        results_4 = pdb_4.keys().collect()

        self.assertTrue('2ONX.A' in results_4)
        self.assertFalse('1JLP.A' in results_4)
        self.assertTrue('5X6H.B' in results_4)
        self.assertFalse('5L2G.A' in results_4)
        self.assertFalse('5L2G.B' in results_4)
        self.assertFalse('2MK1.A' in results_4)
        self.assertFalse('5UZT.A' in results_4)
        self.assertTrue('1AA6.A' in results_4)
        self.assertTrue('1NTH.A' in results_4)

    def test5(self):
        pdb_5 = self.pdb.filter(PolymerComposition(
            PolymerComposition.DNA_STD_NUCLEOTIDES))
        results_5 = pdb_5.keys().collect()

        self.assertFalse('2ONX' in results_5)
        self.assertFalse('1JLP' in results_5)
        self.assertTrue('5X6H' in results_5)
        self.assertFalse('5L2G' in results_5)
        self.assertFalse('2MK1' in results_5)
        self.assertFalse('5UZT' in results_5)

    def test6(self):
        pdb_6 = self.pdb.filter(PolymerComposition(
            PolymerComposition.RNA_STD_NUCLEOTIDES))
        results_6 = pdb_6.keys().collect()

        self.assertFalse('2ONX' in results_6)
        self.assertFalse('1JLP' in results_6)
        self.assertFalse('5X6H' in results_6)
        self.assertFalse('5L2G' in results_6)
        self.assertFalse('2MK1' in results_6)
        self.assertTrue('5UZT' in results_6)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
