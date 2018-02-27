#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.mappers import StructureToBiopython, StructureToPolymerChains
from mmtfPyspark.io.MmtfReader import download_mmtf_files


class StructureToBiopythonTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('structureToBiopythonTest')
        self.sc = SparkContext(conf=conf)

        # 1STP: 1 L-protein chain:
        # 4HHB: 4 polymer chains
        # 1JLP: 1 L-protein chains with non-polymer capping group (NH2)
        # 5X6H: 1 L-protein and 1 DNA chain
        # 5L2G: 2 DNA chain
        # 2MK1: 0 polymer chains
        # --------------------
        # tot: 10 chains

        pdbIds = ["1STP", "4HHB", "1JLP", "5X6H", "5L2G", "2MK1"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):

        chainCounts = self.pdb.flatMapValues(StructureToBiopython()) \
            .values() \
            .map(lambda x: sum(1 for a in x.get_chains()))
        print(chainCounts.sum())

        self.assertTrue(chainCounts.sum() == 10)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
