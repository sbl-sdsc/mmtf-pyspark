#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.webFilters import PdbjMine
from mmtfPyspark.datasets import pdbjMineService
from mmtfPyspark.mappers import StructureToPolymerChains


class MineSearchTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('mineSearchTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["5JDE", "5CU4", "5L6W", "5UFU", "5IHB"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        sql = "select count(*) from brief_summary"

        search = pdbjMineService.get_dataset(sql)

        self.assertTrue(search.head()[0] > 100000)

    def test2(self):
        sql = "select distinct entity.pdbid from entity join entity_src_gen on entity_src_gen.pdbid=entity.pdbid where pdbx_ec='2.7.11.1' and pdbx_gene_src_scientific_name='Homo sapiens'"

        pdb = self.pdb.filter(PdbjMine(sql))
        matches = pdb.keys().collect()

        self.assertTrue("5JDE" in matches)
        self.assertTrue("5CU4" in matches)
        self.assertTrue("5L6W" in matches)
        self.assertFalse("5UFU" in matches)
        self.assertFalse("5IHB" in matches)

    def test3(self):
        sql = "select distinct concat(entity_poly.pdbid, '.', unnest(string_to_array(entity_poly.pdbx_strand_id, ','))) as \"structureChainId\" from entity_poly join entity_src_gen on entity_src_gen.pdbid=entity_poly.pdbid and entity_poly.entity_id=entity_poly.entity_id join entity on entity.pdbid=entity_poly.pdbid and entity.id=entity_poly.entity_id where pdbx_ec='2.7.11.1' and pdbx_gene_src_scientific_name='Homo sapiens'"

        pdb = self.pdb.flatMap(StructureToPolymerChains()) \
                      .filter(PdbjMine(sql, pdbidField="structureChainId", chainLevel=True))

        matches = pdb.keys().collect()

        self.assertTrue("5JDE.A" in matches)
        self.assertTrue("5JDE.B" in matches)
        self.assertTrue("5CU4.A" in matches)
        self.assertTrue("5L6W.L" in matches)
        self.assertFalse("5L6W.C" in matches)
        self.assertFalse("5UFU.A" in matches)
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
