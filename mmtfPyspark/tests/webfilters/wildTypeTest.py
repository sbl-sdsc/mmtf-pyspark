#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.webfilters import WildTypeQuery


class WildTypeTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("wildTypeTest") \
                                 .getOrCreate()
       
        pdbIds = ["1PEN", "1OCZ", "2ONX"]
        self.pdb = download_mmtf_files(pdbIds)

    def test1(self):
        pdb_1 = self.pdb.filter(WildTypeQuery(True, WildTypeQuery.SEQUENCE_COVERAGE_100))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1PEN' in results_1)
        self.assertTrue('1OCZ' in results_1)
        self.assertFalse('2ONX' in results_1)


    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
