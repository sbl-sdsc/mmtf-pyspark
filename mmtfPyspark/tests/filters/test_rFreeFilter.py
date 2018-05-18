#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.filters import RFree


class RFreeFilterTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("RFreeFilterTest") \
                                 .getOrCreate()
        
        # 2ONX: 0.202 rfree x-ray resolution
        # 2OLX: 0.235 rfree x-ray resolution
        # 3REC: n/a NMR structure
        # 1LU3: n/a EM structure
        pdbIds = ['2ONX', '2OLX', '3REC', '1LU3']
        self.pdb = download_mmtf_files(pdbIds)

    def test1(self):
        pdb_1 = self.pdb.filter(RFree(0.201, 0.203))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('2ONX' in results_1)
        self.assertFalse('2OLX' in results_1)
        self.assertFalse('3REC' in results_1)
        self.assertFalse('5KHE' in results_1)

    def test2(self):
        pdb_2 = self.pdb.filter(RFree(0.234, 0.236))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertTrue('2OLX' in results_2)
        self.assertFalse('3REC' in results_2)
        self.assertFalse('5KHE' in results_2)

    def test3(self):
        pdb_3 = self.pdb.filter(RFree(0.15, 0.2))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('2ONX' in results_3)
        self.assertFalse('2OLX' in results_3)
        self.assertFalse('3REC' in results_3)
        self.assertFalse('5KHE' in results_3)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
