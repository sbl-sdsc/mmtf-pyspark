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
from mmtfPyspark.io import mmtfReader


class ReadSequenceFileTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('read_sequence_file')
        self.sc = SparkContext(conf=conf)

    '''
    def test_mmtf(self):
        path = './resources/files/'
        pdb = mmtfReader.read_mmtf_files(path, self.sc)
        self.assertTrue(pdb.count() == 3)
    '''

    def test_read_local_full_sequence(self):
        pdb = mmtfReader.read_full_squence_file(self.sc)
        self.assertTrue(pdb.count() == 4394)

    def test_read_local_reduced_sequence(self):
        pdb = mmtfReader.read_reduced_squence_file(self.sc)
        self.assertTrue(pdb.count() == 5395)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
