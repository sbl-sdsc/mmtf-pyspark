#!/usr/bin/env python

import unittest

from pyspark.sql import SparkSession

from mmtfPyspark.datasets import pdbToUniProt


class PdbToUniProtTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
            .appName("PdbToUniProtTest") \
            .getOrCreate()

    def test1(self):
        ds = pdbToUniProt.get_chain_mappings()
        self.assertGreater(ds.count(), 400000)

    def test2(self):
        ds = pdbToUniProt.get_chain_mappings(['102L'])
        self.assertEqual(2, ds.filter("uniProtId = 'P00720'").count())

    def test3(self):
        ds = pdbToUniProt.get_chain_mappings(['101M.A', '102L.A'])
        self.assertEqual(1, ds.filter("uniProtId = 'P02185'").count())

    def test4(self):
        ds = pdbToUniProt.get_cached_residue_mappings()
        self.assertGreater(ds.count(), 10000000)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
