#!/usr/bin/env python

import unittest

from pyspark.sql import SparkSession

from mmtfPyspark.datasets import dbSnpDataset as dbsnp


class DbSnpDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
            .appName("DbSnpDatasetTest") \
            .getOrCreate()

    def test1(self):
        ds = dbsnp.get_cached_dataset()
        self.assertGreater(ds.count(), 1170000)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
