#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.webfilters import PdbjMineSearch
from mmtfPyspark.datasets import pdbjMineDataset
from mmtfPyspark.mappers import StructureToPolymerChains


class MineSearchTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('mineSearchTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["5JDE", "5CU4", "5L6W", "5UFU", "5IHB"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        sql = "select count(*) from brief_summary"

        search = pdbjMineDataset.get_dataset(sql)

        self.assertTrue(search.head()[0] > 100000)

    def test2(self):
        sql = "SELECT pdbid, chain FROM sifts.pdb_chain_enzyme WHERE ec_number = '2.7.11.1'"

        pdb = self.pdb.filter(PdbjMineSearch(sql))
        matches = pdb.keys().collect()

        self.assertTrue("5JDE" in matches)
        self.assertTrue("5CU4" in matches)
        self.assertTrue("5L6W" in matches)
        self.assertTrue("5UFU" in matches)
        self.assertFalse("5IHB" in matches)
        self.assertFalse("1FIN" in matches)

    def test3(self):

        sql = "SELECT e.pdbid, e.chain FROM sifts.pdb_chain_enzyme AS e WHERE e.ec_number = '2.7.11.1'"
        pdb = self.pdb.flatMap(StructureToPolymerChains()) \
                      .filter(PdbjMineSearch(sql))

        matches = pdb.keys().collect()

        self.assertTrue("5JDE.A" in matches)
        self.assertTrue("5JDE.B" in matches)
        self.assertTrue("5CU4.A" in matches)
        self.assertTrue("5L6W.L" in matches)
        self.assertFalse("5L6W.C" in matches)
        self.assertTrue("5UFU.A" in matches)
        self.assertFalse("5UFU.B" in matches)
        self.assertFalse("5UFU.C" in matches)
        self.assertFalse("5IHB.A" in matches)
        self.assertFalse("5IHB.B" in matches)
        self.assertFalse("5IHB.C" in matches)
        self.assertFalse("5IHB.D" in matches)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
