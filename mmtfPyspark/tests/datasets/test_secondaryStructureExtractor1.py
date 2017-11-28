#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import downloadMmtfFiles
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.filters import containsLProteinChain
from mmtfPyspark.mappers import structureToPolymerChains


class secondaryStructureExtractorTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('secondaryStructureExtractorTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1STP","4HHB"]
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb = self.pdb.filter(containsLProteinChain()) \
                      .flatMap(structureToPolymerChains()) \
                      .filter(containsLProteinChain())

        seq = secondaryStructureExtractor.getDataset(pdb)

        self.assertTrue(seq.count() == 5)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
