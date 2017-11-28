#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import downloadMmtfFiles
from mmtfPyspark.datasets import polymerSequenceExtractor
from mmtfPyspark.mappers import structureToPolymerChains


class polymerSequenceExtractorTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('polymerSequenceExtractorTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1STP","4HHB"]
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb = self.pdb.flatMap(structureToPolymerChains())
        seq = polymerSequenceExtractor.getDataset(pdb)

        self.assertTrue(seq.count() == 5)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
