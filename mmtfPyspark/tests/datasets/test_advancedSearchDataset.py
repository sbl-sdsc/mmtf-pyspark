#!/usr/bin/env python

import unittest

from pyspark.sql import SparkSession

from mmtfPyspark.datasets import advancedSearchDataset


class AdvancedSearchDatasetTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
            .appName("AdvancedSearchDatasetTest") \
            .getOrCreate()

    def test1(self):
        query = (
            "<orgPdbQuery>"
            "<queryType>org.pdb.query.simple.StoichiometryQuery</queryType>"
            "<stoichiometry>A3B3C3</stoichiometry>"
            "</orgPdbQuery>"
        )

        ds = advancedSearchDataset.get_dataset(query)
        self.assertEqual(ds.filter("pdbId = '1A5K'").count(), 1)

    def test2(self):
        query = (
            "<orgPdbQuery>"
            "<queryType>org.pdb.query.simple.TreeEntityQuery</queryType>"
            "<t>1</t>"
            "<n>9606</n>"
            "</orgPdbQuery>"
        )

        ds = advancedSearchDataset.get_dataset(query)
        self.assertEqual(ds.filter("pdbChainId = '10GS.A' OR pdbChainId = '10GS.B'").count(), 2)

    def test3(self):
        query = (
            "<orgPdbQuery>"
            "<queryType>org.pdb.query.simple.ChemSmilesQuery</queryType>"
            "<smiles>CC(C)C1=C(Br)C(=O)C(C)=C(Br)C1=O</smiles>"
            "<target>Ligand</target>"
            "<searchType>Substructure</searchType>"
            "<polymericType>Any</polymericType>"
            "</orgPdbQuery>"
        )

        ds = advancedSearchDataset.get_dataset(query)
        self.assertEqual(ds.filter("ligandId = 'BNT'").count(), 1)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
