#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.datasets import myVariantDataset


class MyVariantDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("MyVariantDatasetTest") \
                                 .getOrCreate()

    def test1(self):
        uniprotIds = ['P00533']    # EGFR
        ds = myVariantDataset.get_variations(uniprotIds)
        self.assertTrue(ds.count() > 7000)

    def test2(self):
        uniprotIds = ['P15056']    # BRAF
        query = "clinvar.rcv.clinical_significance:pathogenic OR clinvar.rcv.clinical_significance:likely pathogenic"
        ds = myVariantDataset.get_variations(uniprotIds, query)
        filter_query ="variationId = 'chr7:g.140501287T>C' AND uniprotId = 'P15056'"
        count = ds.filter(filter_query).count()
        self.assertTrue(count == 1)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
