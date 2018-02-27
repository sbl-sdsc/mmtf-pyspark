#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import download_mmtf_files
from mmtfPyspark.webfilters import chemicalStructureQuery


class ChemicalStructureQueryTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('chemicalStructureQueryTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1HYA", "2ONX", "1F27", "4QMC", "2RTL"]
        self.pdb = download_mmtf_files(pdbIds,self.sc)


    def test1(self):
        smiles = "CC(=O)NC1C(O)OC(CO)C(O)C1O"

        pdb_1 = self.pdb.filter(chemicalStructureQuery(smiles))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1HYA' in results_1)
        self.assertFalse('2ONX' in results_1)


    def test2(self):
        smiles = "OC(=O)CCCC[C@@H]1SC[C@@H]2NC(=O)N[C@H]12"
        queryType = chemicalStructureQuery.EXACT
        percentSimilarity = 0

        pdb_2 = self.pdb.filter(chemicalStructureQuery(smiles, queryType, percentSimilarity))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('1HYA' in results_2)
        self.assertFalse('2ONX' in results_2)
        self.assertTrue('1F27' in results_2)
        self.assertFalse('2RTL' in results_2)
        self.assertFalse('4QMC' in results_2)


    def test3(self):
        smiles = "OC(=O)CCCC[C@@H]1SC[C@@H]2NC(=O)N[C@H]12"
        queryType = chemicalStructureQuery.SUBSTRUCTURE
        percentSimilarity = 0

        pdb_3 = self.pdb.filter(chemicalStructureQuery(smiles, queryType, percentSimilarity))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('1HYA' in results_3)
        self.assertFalse('2ONX' in results_3)
        self.assertTrue('1F27' in results_3)
        self.assertFalse('2RTL' in results_3)
        self.assertTrue('4QMC' in results_3)


    def test4(self):
        smiles = "OC(=O)CCCC[C@@H]1SC[C@@H]2NC(=O)N[C@H]12"
        queryType = chemicalStructureQuery.SIMILAR
        percentSimilarity = 70

        pdb_4 = self.pdb.filter(chemicalStructureQuery(smiles, queryType, percentSimilarity))
        results_4 = pdb_4.keys().collect()

        self.assertFalse('1HYA' in results_4)
        self.assertFalse('2ONX' in results_4)
        self.assertTrue('1F27' in results_4)
        self.assertTrue('2RTL' in results_4)
        self.assertTrue('4QMC' in results_4)


    def test5(self):
        smiles = "OC(=O)CCCC[C@H]1[C@H]2NC(=O)N[C@H]2C[S@@]1=O"
        queryType = chemicalStructureQuery.SUPERSTRUCTURE
        percentSimilarity = 0

        pdb_5 = self.pdb.filter(chemicalStructureQuery(smiles, queryType, percentSimilarity))
        results_5 = pdb_5.keys().collect()

        self.assertFalse('1HYA' in results_5)
        self.assertFalse('2ONX' in results_5)
        self.assertTrue('1F27' in results_5)
        self.assertFalse('2RTL' in results_5)
        self.assertTrue('4QMC' in results_5)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
