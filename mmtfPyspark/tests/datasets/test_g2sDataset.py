#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.datasets import g2sDataset


class g2sDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("g2sDatasetTest") \
                                 .getOrCreate()

    def test1(self):
        variantIds = ['chr7:g.140449098T>C']
        ds = g2sDataset.get_position_dataset(variantIds, '3TV4', 'A')
        self.assertTrue(ds.filter("pdbPosition = 661").count() == 1)

    def test2(self):
        variantIds = ['chr7:g.140449098T>C']
        ds = g2sDataset.get_full_dataset(variantIds, '3TV4', 'A')
        self.assertTrue(ds.filter("pdbPosition = 661").count() > 1)

    def test3(self):
        variantIds = ['chr7:g.140449098T>C']
        ds = g2sDataset.get_position_dataset(variantIds, '1STP', 'A')
        self.assertTrue(ds == None)

    def test4(self):
        variantIds = ['chr7:g.140449098T>C']
        ds = g2sDataset.get_position_dataset(variantIds)
        self.assertTrue(ds.count() > 5)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
