#!/usr/bin/env python
'''
readSequenceFromListTest.py: Example reading a list of PDB IDs from a local MMTF Hadoop sequence \
file into a tubleRDD.

Authorship information:
__author__ = "Mars Huang"
__maintainer__ = "Mars Huang"
__email__ = "marshuang80@gmail.com:
__status__ = "Warning"
'''

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import MmtfReader
from mmtfPyspark.mappers import structureToPolymerChains


class testReadSequenceFile(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('readSequenceFile')
        self.sc = SparkContext(conf=conf)

    def test_mmtf(self):
        path = './resources/files/'
        pdb = MmtfReader.readMmtfFiles(path, self.sc)

        self.assertTrue(pdb.count() == 3)


    def test_pdb(self):
        path = './resources/files/'
        pdb = MmtfReader.readPDBFiles(path, self.sc)

        self.assertTrue(pdb.count() == 3)


    def test_mmcif(self):
        path = './resources/files/'
        pdb = MmtfReader.readMmcifFiles(path, self.sc)

        self.assertTrue(pdb.count() == 2)


    def test_mmtf_chains(self):
        path = './resources/files/test'
        pdb = MmtfReader.readMmtfFiles(path, self.sc)
        self.assertTrue(pdb.count() == 1)

        pdb_chains = pdb.flatMap(structureToPolymerChains())
        self.assertTrue(pdb_chains.count()== 8)


    def tearDown(self):
        self.sc.stop()

if __name__ == '__main__':
    unittest.main()
