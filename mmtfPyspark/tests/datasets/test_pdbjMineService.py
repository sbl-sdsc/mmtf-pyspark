#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.datasets import pdbjMineService


class PdbjMineSearchDatasetTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]") \
                          .setAppName('pdbjMineDatasetTest')
        self.sc = SparkContext(conf=conf)

    def test1(self):

        sql = "SELECT * FROM sifts.pdb_chain_uniprot LIMIT 10"
        ds = pdbjMineService.get_dataset(sql)

        count = ds.filter("structureChainId == '101M.A'").count()
        self.assertTrue(count == 1)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
