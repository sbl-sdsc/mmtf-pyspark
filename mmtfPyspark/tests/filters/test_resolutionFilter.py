#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.filters import Resolution


class ResolutionFilterTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("testResolutionFilter") \
                                 .getOrCreate()

        # 2ONX: 1.52 A x-ray resolution
        # 2OLX: 1.42 A x-ray resolution
        # 3REC: n/a NMR structure
        # 1LU3: 16.8 A EM resolution
        pdbIds = ['2ONX', '2OLX', '3REC', '1LU3']
        self.pdb = download_mmtf_files(pdbIds)

    def test1(self):
        pdb_1 = self.pdb.filter(Resolution(1.51, 1.53))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('2ONX' in results_1)
        self.assertFalse('2OLX' in results_1)
        self.assertFalse('3REC' in results_1)
        self.assertFalse('1LU3' in results_1)

    def test2(self):
        pdb_2 = self.pdb.filter(Resolution(1.41, 1.43))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertTrue('2OLX' in results_2)
        self.assertFalse('3REC' in results_2)
        self.assertFalse('1LU3' in results_2)

    def test3(self):
        pdb_3 = self.pdb.filter(Resolution(34.99, 35.01))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('2ONX' in results_3)
        self.assertFalse('2OLX' in results_3)
        self.assertFalse('3REC' in results_3)
        self.assertFalse('1LU3' in results_3)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
