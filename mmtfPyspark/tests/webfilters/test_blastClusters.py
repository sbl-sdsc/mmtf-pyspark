#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import download_mmtf_files
from mmtfPyspark.webfilters import blastCluster
from mmtfPyspark.mappers import StructureToPolymerChains


class BlastClustersTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('blastClustersTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1O06", "2ONX"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):

        pdb_1 = self.pdb.filter(blastCluster(40))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1O06' in results_1)
        self.assertFalse('1O06.A' in results_1)
        self.assertFalse('2ONX' in results_1)

    def test2(self):

        pdb_2 = self.pdb.filter(blastCluster(40))
        pdb_2 = pdb_2.flatMap(StructureToPolymerChains())
        results_2 = pdb_2.keys().collect()

        self.assertFalse('1O06' in results_2)
        self.assertTrue('1O06.A' in results_2)
        self.assertFalse('2ONX' in results_2)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
