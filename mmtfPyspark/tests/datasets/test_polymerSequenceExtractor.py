#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import polymerSequenceExtractor
from mmtfPyspark.mappers import StructureToPolymerChains


class PolymerSequenceExtractorTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("polymerSequenceExtractorTest") \
                                 .getOrCreate()

        pdbIds = ["1STP","4HHB"]
        self.pdb = download_mmtf_files(pdbIds)


    def test1(self):
        pdb = self.pdb.flatMap(StructureToPolymerChains())
        seq = polymerSequenceExtractor.get_dataset(pdb)

        self.assertTrue(seq.count() == 5)


    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
