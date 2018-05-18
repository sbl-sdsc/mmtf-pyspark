#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.datasets import drugBankDataset


class DurgBankDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("DrugBankDatasetTest") \
                                 .getOrCreate()
        self.ds = drugBankDataset.get_open_drug_links()

    def test1(self):

        self.assertTrue(self.ds.count() > 10000)

    def test2(self):

        self.assertTrue(self.ds.columns[0] == "DrugBankID")

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
