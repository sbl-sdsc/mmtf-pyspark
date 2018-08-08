#!/usr/bin/env python

import unittest

from pyspark.sql import SparkSession

from mmtfPyspark.datasets import dbPtmDataset as pm
from mmtfPyspark.datasets.dbPtmDataset import PtmType


class DbPtmDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
            .appName("DbPtmDatasetTest") \
            .getOrCreate()

    def test1(self):
        ds = pm.download_ptm_dataset(PtmType.S_LINKEDGLYCOSYLATION)
        self.assertGreater(ds.count(), 4)

    def test2(self):
        ds = pm.get_ptm_dataset()
        self.assertGreater(ds.count(), 900000)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
