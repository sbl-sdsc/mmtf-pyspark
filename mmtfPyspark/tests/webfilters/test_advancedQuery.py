#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import download_mmtf_files
from mmtfPyspark.webfilters import advancedQuery
from mmtfPyspark.mappers import StructureToPolymerChains


class AdvancedQueryTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('advancedQueryTest')
        self.sc = SparkContext(conf=conf)

    	# 1PEN wildtype query 100 matches: 1PEN:1
	    # 1OCZ two entities wildtype query 100 matches: 1OCZ:1, 1OCZ:2
		# 2ONX structure result for author query
		# 5L6W two chains: chain L is EC 2.7.11.1, chain chain C is not EC 2.7.11.1
		# 5KHU many chains, chain Q is EC 2.7.11.1
		# 1F3M entity 1: chains A,B, entity 2: chains B,C, all chains are EC 2.7.11.1
        pdbIds = ["1PEN","1OCZ","2ONX","5L6W","5KHU","1F3M"]
        self.pdb = download_mmtf_files(pdbIds,self.sc)


    def test1(self):
        query = "<orgPdbQuery>" + \
	                "<queryType>org.pdb.query.simple.WildTypeProteinQuery</queryType>" + \
				    "<includeExprTag>Y</includeExprTag>" + \
	                "<percentSeqAlignment>100</percentSeqAlignment>" + \
				"</orgPdbQuery>";

        pdb_1 = self.pdb.filter(advancedQuery(query))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1PEN' in results_1)
        self.assertTrue('1OCZ' in results_1)
        self.assertFalse('2ONX' in results_1)
        self.assertFalse('5L6W' in results_1)


    def test2(self):
        query = "<orgPdbQuery>" + \
				    "<queryType>org.pdb.query.simple.AdvancedAuthorQuery</queryType>" + \
				    "<searchType>All Authors</searchType><audit_author.name>Eisenberg</audit_author.name>" + \
				    "<exactMatch>false</exactMatch>" + \
				"</orgPdbQuery>";

        pdb_2 = self.pdb.filter(advancedQuery(query))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('1PEN' in results_2)
        self.assertFalse('1OCZ' in results_2)
        self.assertTrue('2ONX' in results_2)
        self.assertFalse('5L6W' in results_2)


    def test3(self):
        query = "<orgPdbQuery>" + \
				    "<queryType>org.pdb.query.simple.EnzymeClassificationQuery</queryType>" + \
				    "<Enzyme_Classification>2.7.11.1</Enzyme_Classification>" + \
				"</orgPdbQuery>";

        pdb_3 = self.pdb.filter(advancedQuery(query))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('1PEN' in results_3)
        self.assertFalse('1OCZ' in results_3)
        self.assertFalse('2ONX' in results_3)
        self.assertTrue('5L6W' in results_3)
        self.assertTrue('5KHU' in results_3)



    def test4(self):
        query = "<orgPdbQuery>" + \
				    "<queryType>org.pdb.query.simple.EnzymeClassificationQuery</queryType>" + \
				    "<Enzyme_Classification>2.7.11.1</Enzyme_Classification>" + \
				"</orgPdbQuery>"

        pdb_4 = self.pdb.flatMap(StructureToPolymerChains()) \
                        .filter(advancedQuery(query))
        results_4 = pdb_4.keys().collect()

        self.assertFalse('1PEN.A' in results_4)
        self.assertFalse('1OCZ.A' in results_4)
        self.assertFalse('2ONX.A' in results_4)
        self.assertTrue('5L6W.L' in results_4)
        self.assertFalse('5L6W.C' in results_4)
        self.assertFalse('5KHU.A' in results_4)
        self.assertFalse('5KHU.B' in results_4)
        self.assertTrue('5KHU.Q' in results_4)
        self.assertTrue('1F3M.A' in results_4)
        self.assertTrue('1F3M.B' in results_4)
        self.assertTrue('1F3M.C' in results_4)
        self.assertTrue('1F3M.D' in results_4)



    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
