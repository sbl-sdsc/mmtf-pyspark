#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import download_mmtf_files
from mmtfPyspark.filters import ContainsGroup


class ContainsGroupTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('ContainsGroupTest')
        self.sc = SparkContext(conf=conf)

        # 1STP: only L-protein chain
        # 1JLP: single L-protein chains with non-polymer capping group (NH2)
        # 5X6H: L-protein and DNA chain
        # 5L2G: DNA chain
        # 2MK1: D-saccharide
        pdbIds = ['1STP', '1JLP', '5X6H', '5L2G', '2MK1']
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        pdb_1 = self.pdb.filter(ContainsGroup('BTN'))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1STP' in results_1)
        self.assertFalse('1JLP' in results_1)
        self.assertFalse('5X6H' in results_1)
        self.assertFalse('5L2G' in results_1)
        self.assertFalse('2MK1' in results_1)

    def test2(self):
        pdb_2 = self.pdb.filter(ContainsGroup('HYP'))
        results_2 = pdb_2.keys().collect()

        self.assertFalse("1STP" in results_2)
        self.assertTrue("1JLP" in results_2)
        self.assertFalse("5X6H" in results_2)
        self.assertFalse("5L2G" in results_2)
        self.assertFalse("2MK1" in results_2)

    def test3(self):
        pdb_3 = self.pdb.filter(ContainsGroup('HYP', 'NH2'))
        results_3 = pdb_3.keys().collect()

        self.assertFalse("1STP" in results_3)
        self.assertTrue("1JLP" in results_3)
        self.assertFalse("5X6H" in results_3)
        self.assertFalse("5L2G" in results_3)
        self.assertFalse("2MK1" in results_3)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
