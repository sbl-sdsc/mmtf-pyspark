#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.mappers import StructureToPolymerChains


class SecondaryStructureExtractorTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('secondaryStructureExtractorTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1STP"]
        self.pdb = download_mmtf_files(pdbIds,self.sc)


    def test1(self):
        pdb = self.pdb.flatMap(StructureToPolymerChains())

        secStruct = secondaryStructureExtractor.get_dataset(pdb)

        dsspQ8 = secStruct.first()["dsspQ8Code"]
        dsspQ3 = secStruct.first()["dsspQ3Code"]

        self.assertTrue(len(dsspQ8.split('X')) - 1 == 38) # 'X' appears 38 times
        self.assertTrue(len(dsspQ8.split('C')) - 1 == 24) # 'C' appears 24 times
        self.assertTrue(len(dsspQ3.split('C')) - 1 == 44) # 'C' appears 44 times


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
