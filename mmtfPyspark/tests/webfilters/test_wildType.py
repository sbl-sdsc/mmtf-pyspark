#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import download_mmtf_files
from mmtfPyspark.webfilters import wildTypeQuery


class wildTypeTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('wildTypeTest')
        self.sc = SparkContext(conf=conf)

        pdbIds = ["1PEN", "1OCZ", "2ONX"]
        self.pdb = download_mmtf_files(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(wildTypeQuery(True, wildTypeQuery.SEQUENCE_COVERAGE_100))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('1PEN' in results_1)
        self.assertTrue('1OCZ' in results_1)
        self.assertFalse('2ONX' in results_1)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
