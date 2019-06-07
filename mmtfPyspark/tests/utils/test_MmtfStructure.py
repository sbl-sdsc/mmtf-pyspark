#!/usr/bin/env python
'''

Authorship information:
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__status__ = "Warning"
'''

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io import mmtfReader


class TestMmtfStructure(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("TestMmtfStructure") \
                                 .getOrCreate()

    def test_4HHB_structure(self):
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        self.assertEqual(4, structure.num_atoms, 4779)
        self.assertEqual(4, structure.num_groups, 620)
        self.assertEqual(4, structure.num_chains, 14)
        self.assertEqual(4, structure.num_models, 1)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
