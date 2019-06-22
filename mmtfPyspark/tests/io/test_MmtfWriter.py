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
import tempfile
from pyspark.sql import SparkSession
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.io import mmtfWriter


class WriteSequenceFileTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("write_sequence_file").getOrCreate()

    def test_mmtf(self):
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        self.assertEqual(4, pdb.count())
        tmp_path = tempfile.mkdtemp()
        print(tmp_path)
        mmtfWriter.write_mmtf_files(tmp_path, pdb)
        #pdb = mmtfReader.read_mmtf_files(tmp_path)
        #self.assertTrue(pdb.count() == 3)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
