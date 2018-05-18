#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import pdbjMineDataset


class PdbjMineSearchDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("pdbjMineDatasetTest") \
                                 .getOrCreate()

    def test1(self):

        sql = "SELECT * FROM sifts.pdb_chain_uniprot LIMIT 10"
        ds = pdbjMineDataset.get_dataset(sql)

        count = ds.filter("structureChainId == '101M.A'").count()
        self.assertTrue(count == 1)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
