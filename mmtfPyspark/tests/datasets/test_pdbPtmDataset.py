#!/usr/bin/env python

import unittest

from pyspark.sql import SparkSession

from mmtfPyspark.datasets import pdbPtmDataset as pm


class PdbPtmDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
            .appName("PdbPtmDatasetTest") \
            .getOrCreate()

    def test1(self):
        ds = pm.get_ptm_dataset()
        ds.show()
        self.assertGreater(ds.count(), 1100000)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
