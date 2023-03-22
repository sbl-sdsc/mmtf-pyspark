#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.filters import ContainsAlternativeLocations


class ContainsAlternativeLocationsTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("ContainsAlternativeLocationsTests") \
                                 .getOrCreate()

        # 4QXX: has alternative location ids
        # 2ONX: has no alternative location ids
        pdbIds = ['4QXX', '2ONX']
        self.pdb = download_mmtf_files(pdbIds)
        self.pdb = self.pdb.map(lambda x: x)

    def test1(self):
        pdb_1 = self.pdb.filter(ContainsAlternativeLocations())
        results_1 = pdb_1.keys().collect()

        self.assertTrue('4QXX' in results_1)
        self.assertFalse('2ONX' in results_1)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
