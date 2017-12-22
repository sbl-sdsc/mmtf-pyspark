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
from mmtfPyspark.io.MmtfReader import readSequenceFile


class testReadSequenceFile(unittest.TestCase):

    def setUp(self):
        path = 'resources/sample_rdd'
        stringIds = "1FDK,1FDL,1FDM,1FDN,1FDO,1FDP,1FDQ,1FDR,1FDS,1FDT"

        self.pdbIds = stringIds.split(',')
        conf = SparkConf().setMaster("local[*]").setAppName('readSequenceFile')
        self.sc = SparkContext(conf=conf)
        self.pdb = readSequenceFile(path, self.sc, pdbId = self.pdbIds)


    def test_size(self):
        self.assertEqual(len(self.pdbIds),self.pdb.count())


    def test_result(self):
        self.assertEqual(set(self.pdbIds),set(self.pdb.keys().collect()))


    def tearDown(self):
        self.sc.stop()

if __name__ == '__main__':
    unittest.main()
