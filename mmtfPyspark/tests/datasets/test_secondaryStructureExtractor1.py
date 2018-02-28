#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.mappers import StructureToPolymerChains


class secondaryStructureExtractorTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('secondaryStructureExtractorTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1STP","4HHB"]
        self.pdb = download_mmtf_files(pdbIds,self.sc)


    def test1(self):
        pdb = self.pdb.filter(ContainsLProteinChain()) \
                      .flatMap(StructureToPolymerChains()) \
                      .filter(ContainsLProteinChain())

        seq = secondaryStructureExtractor.getDataset(pdb)

        self.assertTrue(seq.count() == 5)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
