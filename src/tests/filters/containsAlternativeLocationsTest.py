#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from src.main.MmtfReader import downloadMmtfFiles
from src.main.filters import containsAlternativeLocations

class testContainsAlternativeLocations(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testContainsAlternativeLocations')
        self.sc = SparkContext(conf=conf)

        # 4QXX: has alternative location ids
        # 2ONX: has no alternative location ids
        pdbIds = ['4QXX','2ONX']
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(containsAlternativeLocations())
        results_1 = pdb_1.keys().collect()

        self.assertTrue('4QXX' in results_1)
        self.assertFalse('2ONX' in results_1)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
