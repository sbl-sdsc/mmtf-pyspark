#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.mappers import StructureToPolymerChains


class SecondaryStructureExtractorTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("secondaryStructureExtractorTest") \
                                 .getOrCreate()

        pdbIds = ["1STP","4HHB"]
        self.pdb = download_mmtf_files(pdbIds)


    def test1(self):
        pdb = self.pdb.filter(ContainsLProteinChain()) \
                      .flatMap(StructureToPolymerChains()) \
                      .filter(ContainsLProteinChain())

        seq = secondaryStructureExtractor.get_dataset(pdb)

        self.assertTrue(seq.count() == 5)


    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
