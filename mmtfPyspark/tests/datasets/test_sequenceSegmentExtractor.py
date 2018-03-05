#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import secondaryStructureSegmentExtractor
from mmtfPyspark.mappers import StructureToPolymerChains


class SecondaryStructureSegmentExtractorTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('secondaryStructureSegmentExtractorTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1STP"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        pdb = self.pdb.flatMap(StructureToPolymerChains())

        seq = secondaryStructureSegmentExtractor.get_dataset(pdb, 25)

        self.assertTrue("DPSKDSKAQVSAAEAGITGTWYNQL" == seq.head()[1])

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
