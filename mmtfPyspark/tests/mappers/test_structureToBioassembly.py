#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.mappers import StructureToBioassembly
from mmtfPyspark.io.mmtfReader import download_mmtf_files


class StructureToBioassemblyTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("structureToBioassemblyTest") \
                                 .getOrCreate()
        
        # 1STP: 1 L-protein chain:
        # 4HHB: 4 polymer chains
        # 1JLP: 1 L-protein chains with non-polymer capping group (NH2)
        # 5X6H: 1 L-protein and 1 DNA chain
        # 5L2G: 2 DNA chain
        # 2MK1: 0 polymer chains
        # --------------------
        # tot: 10 chains ,"4HHB","1JLP","5X6H","5L2G","2MK1"
        pdbIds = ["1HV4"]
        self.pdb = download_mmtf_files(pdbIds)

    def test1(self):
        pdb_1 = self.pdb.flatMap(StructureToBioassembly())
        results_1 = pdb_1.keys().collect()

        self.assertTrue(len(results_1) == 2)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
