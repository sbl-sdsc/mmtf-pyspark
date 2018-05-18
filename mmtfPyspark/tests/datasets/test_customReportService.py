#!/usr/bin/env python

import unittest

from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import customReportService
from pyspark.sql import SparkSession

class CustomReportServiceTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("customReportServiceTest") \
                                 .getOrCreate()
                                 
    def test1(self):

        ds = customReportService.get_dataset(
            ["pmc", "pubmedId", "depositionDate"])
        self.assertTrue(str(ds.schema) == "StructType(List(StructField(structureId,StringType,true),StructField(pmc,StringType,true),StructField(pubmedId,IntegerType,true),StructField(depositionDate,TimestampType,true)))")
        self.assertTrue(ds.count() > 130101)

    def test2(self):

        ds = customReportService.get_dataset(["ecNo"])
        self.assertTrue(str(ds.schema) == "StructType(List(StructField(structureChainId,StringType,true),StructField(structureId,StringType,true),StructField(chainId,StringType,true),StructField(ecNo,StringType,true)))")
        self.assertTrue(ds.count() > 130101)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
