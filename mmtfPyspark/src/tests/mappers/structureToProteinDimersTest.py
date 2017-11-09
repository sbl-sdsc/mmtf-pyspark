#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from src.main.mappers import structureToBioassembly, structureToProteinDimers
from src.main.io.MmtfReader import downloadMmtfFiles
from src.main.filters import containsAlternativeLocations

class structureToProteinDimersTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('structureToProteinDimers')
        self.sc = SparkContext(conf=conf)


    def test1(self):
        pdbIds = ["1I1G"]
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)
        #print(self.pdb.collect()[0][1].group_type_list)


        pdb_1 = self.pdb.flatMap(structureToBioassembly()) \
                        .flatMap(structureToProteinDimers(8,20,False, True))
        results_1 = pdb_1.keys().collect()
        print(results_1)
        self.assertTrue(len(results_1) == 12)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
