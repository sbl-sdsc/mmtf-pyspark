#!/usr/bin/env python
'''
readSequenceFromListTest.py: Example reading a list of PDB IDs from a local MMTF Hadoop sequence \
file into a tubleRDD.

Authorship information:
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com:
__status__ = "Warning"
'''

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import read_sequence_file


class testReadSequenceFile(unittest.TestCase):

    def setUp(self):
        path = 'resources/sample_rdd'
        stringIds = "1FDK,1FDL,1FDM,1FDN,1FDO,1FDP,1FDQ,1FDR,1FDS,1FDT"

        self.pdbIds = stringIds.split(',')
        conf = SparkConf().setMaster("local[*]").setAppName('read_sequence_file')
        self.sc = SparkContext(conf=conf)
        self.pdb = read_sequence_file(path, self.sc, pdbId = self.pdbIds)


    def test_size(self):
        self.assertEqual(len(self.pdbIds),self.pdb.count())


    def test_result(self):
        self.assertEqual(set(self.pdbIds),set(self.pdb.keys().collect()))


    def tearDown(self):
        self.sc.stop()

if __name__ == '__main__':
    unittest.main()
