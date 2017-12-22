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
        path = '/home/marshuang80/PDB/full'
        stringIds = "1AQ1,1B38,1B39,1BUH,1C25,1CKP,1DI8,1DM2,1E1V,1E1X,1E9H,1F5Q,1FIN,1FPZ,1FQ1,1FQV,1FS1"
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
