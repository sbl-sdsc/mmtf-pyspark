#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import downloadMmtfFiles
from mmtfPyspark.filters import rWork

class testResolutionFilter(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('testRWorkFilter')
        self.sc = SparkContext(conf=conf)

        # 2ONX: 0.172 rwork x-ray resolution
        # 2OLX: 0.183 rfree x-ray resolution
        # 3REC: n/a NMR structure
        # 1LU3: n/a EM structure
        pdbIds = ['2ONX','2OLX','3REC','1LU3']
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(rWork(0.171, 0.173))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('2ONX' in results_1)
        self.assertFalse('2OLX' in results_1)
        self.assertFalse('3REC' in results_1)
        self.assertFalse('1LU3' in results_1)


    def test2(self):
        pdb_2 = self.pdb.filter(rWork(0.182, 0.184))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertTrue('2OLX' in results_2)
        self.assertFalse('3REC' in results_2)
        self.assertFalse('1LU3' in results_2)


    def test3(self):
        pdb_3 = self.pdb.filter(rWork(0.10, 0.16))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('2ONX' in results_3)
        self.assertFalse('2OLX' in results_3)
        self.assertFalse('3REC' in results_3)
        self.assertFalse('1LU3' in results_3)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
