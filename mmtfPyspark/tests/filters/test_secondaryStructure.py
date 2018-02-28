#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.filters import SecondaryStructure
from mmtfPyspark.mappers import StructureToPolymerChains


class SecondaryStructureTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('SecondaryStructureTest')
        self.sc = SparkContext(conf=conf)

        # 1AIE: all alpha protein 20 alpha out of 31 = 0.645 helical
        # 1E0N: all beta protein, NMR structure with 10 models, 13 beta out of 27 = 0.481 sheet
        # 1EM7: alpha + beta, 14 alpha + 23 beta out of 56 = 0.25 helical and 0.411 sheet
        # 2C7M: 2 chains, alpha + beta (DSSP in MMTF doesn't match DSSP on RCSB PDB website)
        pdbIds = ["1AIE", "1E0N", "1EM7", "2C7M"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        pdb_1 = self.pdb.filter(SecondaryStructure(
            0.64, 0.65, 0.0, 0.0, 0.35, 0.36))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1AIE' in results_1)
        self.assertFalse('1E0N' in results_1)
        self.assertFalse('1EM7' in results_1)
        self.assertFalse('2CTM' in results_1)

    def test2(self):
        pdb_2 = self.pdb.filter(SecondaryStructure(
            0.0, 0.0, 0.48, 0.49, 0.51, 0.52))
        results_2 = pdb_2.keys().collect()

        self.assertTrue('1E0N' in results_2)

    def test3(self):
        pdb_3 = self.pdb.filter(SecondaryStructure(
            0.24, 0.26, 0.41, 0.42, 0.33, 0.34))
        results_3 = pdb_3.keys().collect()

        self.assertTrue('1EM7' in results_3)

    def test4(self):
        pdb_4 = self.pdb.filter(SecondaryStructure(0.70, 0.80, 0.00, 0.20, 0.20, 0.30,
                                                   exclusive=True))
        results_4 = pdb_4.keys().collect()

        self.assertFalse('2C7M' in results_4)

    def test5(self):
        pdb_5 = self.pdb.flatMap(StructureToPolymerChains())
        pdb_5 = pdb_5.filter(SecondaryStructure(
            0.25, 0.75, 0.00, 0.40, 0.25, 0.50))
        results_5 = pdb_5.keys().collect()

        self.assertTrue('2C7M.A' in results_5)
        self.assertTrue('2C7M.B' in results_5)

    def test6(self):
        pdb_6 = self.pdb.flatMap(StructureToPolymerChains())
        pdb_6 = pdb_6.filter(SecondaryStructure(
            0.70, 0.75, 0.00, 0.40, 0.25, 0.50))
        results_6 = pdb_6.keys().collect()

        self.assertTrue('2C7M.A' in results_6)
        self.assertFalse('2C7M.B' in results_6)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
