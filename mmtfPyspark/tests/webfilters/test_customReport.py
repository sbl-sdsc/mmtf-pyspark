#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.webFilters import CustomReportQuery
from mmtfPyspark.mappers import StructureToPolymerChains


class CustomReportQueryTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('CustomReportQueryTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["5JDE", "5CU4", "5L6W", "5UFU", "5IHB"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

    def test1(self):
        # This test runs a chain levle query and compares the results at the PDB entry level
        whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'"
        fields = ["ecNo", "source"]
        pdb_1 = self.pdb.filter(CustomReportQuery(whereClause, fields))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('5JDE' in results_1)
        self.assertTrue('5CU4' in results_1)
        self.assertTrue('5L6W' in results_1)
        self.assertFalse('5UFU' in results_1)
        self.assertFalse('5IHB' in results_1)

    def test2(self):
        # This test runs a chain elvel query and compares chain level results
        pdb_2 = self.pdb.flatMap(StructureToPolymerChains())

        whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'"
        fields = ["ecNo", "source"]
        pdb_2 = pdb_2.filter(CustomReportQuery(whereClause, fields))
        results_2 = pdb_2.keys().collect()

        self.assertTrue('5JDE.A' in results_2)
        self.assertTrue('5JDE.B' in results_2)
        self.assertTrue('5CU4.A' in results_2)
        self.assertTrue('5L6W.L' in results_2)
        self.assertFalse('5L6W.C' in results_2)
        self.assertFalse('5UFU.A' in results_2)
        self.assertFalse('5UFU.B' in results_2)
        self.assertFalse('5UFU.C' in results_2)
        self.assertFalse('5IHB.A' in results_2)
        self.assertFalse('5IHB.B' in results_2)
        self.assertFalse('5IHB.C' in results_2)
        self.assertFalse('5IHB.D' in results_2)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
