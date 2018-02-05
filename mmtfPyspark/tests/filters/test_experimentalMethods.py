#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io.MmtfReader import downloadMmtfFiles
from mmtfPyspark.filters import experimentalMethods
from mmtfPyspark.mappers import *

class experimentalMethodsTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('containsDProteinChainTest')
        pdbIds = ["2ONX","5VLN","5VAI","5JXV","5K7N","3PDM","5MNX","5I1R","5MON","5LCB","3J07"]
        self.sc = SparkContext(conf=conf)
        self.pdb = downloadMmtfFiles(pdbIds,self.sc)


    def test1(self):
        pdb_1 = self.pdb.filter(experimentalMethods(experimentalMethods.X_RAY_DIFFRACTION))
        results_1 = pdb_1.keys().collect()

        self.assertTrue('2ONX' in results_1)
        self.assertFalse('5VLN' in results_1)
        self.assertFalse('5VAI' in results_1)
        self.assertFalse('5JXV' in results_1)
        self.assertFalse('5K7N' in results_1)
        self.assertFalse('3PDM' in results_1)
        self.assertFalse('5MNX' in results_1)
        self.assertFalse('5I1R' in results_1)
        self.assertTrue('5MON' in results_1)
        self.assertFalse('5LCB' in results_1)
        self.assertFalse('3J07' in results_1)


    def test1a(self):
        pdb_1a = self.pdb.flatMap(structureToPolymerChains())
        pdb_1a = pdb_1a.filter(experimentalMethods(experimentalMethods.X_RAY_DIFFRACTION))
        results_1a = pdb_1a.keys().collect()

        self.assertTrue('2ONX.A' in results_1a)
        self.assertFalse('5VLN.A' in results_1a)


    def test2(self):
        pdb_2 = self.pdb.filter(experimentalMethods(experimentalMethods.SOLUTION_NMR))
        results_2 = pdb_2.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertTrue('5VLN' in results_2)
        self.assertFalse('5VAI' in results_2)
        self.assertFalse('5JXV' in results_2)
        self.assertFalse('5K7N' in results_2)
        self.assertFalse('3PDM' in results_2)
        self.assertFalse('5MNX' in results_2)
        self.assertTrue('5I1R' in results_2)
        self.assertFalse('5MON' in results_2)
        self.assertFalse('5LCB' in results_2)
        self.assertFalse('3J07' in results_2)


    def test3(self):
        pdb_3 = self.pdb.filter(experimentalMethods(experimentalMethods.ELECTRON_MICROSCOPY))
        results_3 = pdb_3.keys().collect()

        self.assertFalse('2ONX' in results_3)
        self.assertFalse('5VLN' in results_3)
        self.assertTrue('5VAI' in results_3)
        self.assertFalse('5JXV' in results_3)
        self.assertFalse('5K7N' in results_3)
        self.assertFalse('3PDM' in results_3)
        self.assertFalse('5MNX' in results_3)
        self.assertFalse('5I1R' in results_3)
        self.assertFalse('5MON' in results_3)
        self.assertTrue('5LCB' in results_3)
        self.assertTrue('3J07' in results_3)


    def test4(self):
        pdb_4 = self.pdb.filter(experimentalMethods(experimentalMethods.SOLID_STATE_NMR))
        results_4 = pdb_4.keys().collect()

        self.assertFalse('2ONX' in results_4)
        self.assertFalse('5VLN' in results_4)
        self.assertFalse('5VAI' in results_4)
        self.assertTrue('5JXV' in results_4)
        self.assertFalse('5K7N' in results_4)
        self.assertFalse('3PDM' in results_4)
        self.assertFalse('5MNX' in results_4)
        self.assertFalse('5I1R' in results_4)
        self.assertFalse('5MON' in results_4)
        self.assertTrue('5LCB' in results_4)
        self.assertTrue('3J07' in results_4)


    def test5(self):
        pdb_5 = self.pdb.filter(experimentalMethods(experimentalMethods.ELECTRON_CRYSTALLOGRAPHY))
        results_2 = pdb_5.keys().collect()

        self.assertFalse('2ONX' in results_2)
        self.assertFalse('5VLN' in results_2)
        self.assertFalse('5VAI' in results_2)
        self.assertFalse('5JXV' in results_2)
        self.assertTrue('5K7N' in results_2)
        self.assertFalse('3PDM' in results_2)
        self.assertFalse('5MNX' in results_2)
        self.assertFalse('5I1R' in results_2)
        self.assertFalse('5MON' in results_2)
        self.assertFalse('5LCB' in results_2)
        self.assertFalse('3J07' in results_2)


    def test6(self):
        pdb_6 = self.pdb.filter(experimentalMethods(experimentalMethods.FIBER_DIFFRACTION))
        results_6 = pdb_6.keys().collect()

        self.assertFalse('2ONX' in results_6)
        self.assertFalse('5VLN' in results_6)
        self.assertFalse('5VAI' in results_6)
        self.assertFalse('5JXV' in results_6)
        self.assertFalse('5K7N' in results_6)
        self.assertTrue('3PDM' in results_6)
        self.assertFalse('5MNX' in results_6)
        self.assertFalse('5I1R' in results_6)
        self.assertFalse('5MON' in results_6)
        self.assertFalse('5LCB' in results_6)
        self.assertFalse('3J07' in results_6)


    def test7(self):
        pdb_7 = self.pdb.filter(experimentalMethods(experimentalMethods.NEUTRON_DIFFRACTION))
        results_7 = pdb_7.keys().collect()

        self.assertFalse('2ONX' in results_7)
        self.assertFalse('5VLN' in results_7)
        self.assertFalse('5VAI' in results_7)
        self.assertFalse('5JXV' in results_7)
        self.assertFalse('5K7N' in results_7)
        self.assertFalse('3PDM' in results_7)
        self.assertTrue('5MNX' in results_7)
        self.assertFalse('5I1R' in results_7)
        self.assertTrue('5MON' in results_7)
        self.assertFalse('5LCB' in results_7)
        self.assertFalse('3J07' in results_7)


    def test8(self):
        pdb_8 = self.pdb.filter(experimentalMethods(experimentalMethods.SOLUTION_SCATTERING))\
                        .filter(experimentalMethods(experimentalMethods.SOLUTION_NMR))
        results_1 = pdb_8.keys().collect()

        self.assertFalse('2ONX' in results_1)
        self.assertFalse('5VLN' in results_1)
        self.assertFalse('5VAI' in results_1)
        self.assertFalse('5JXV' in results_1)
        self.assertFalse('5K7N' in results_1)
        self.assertFalse('3PDM' in results_1)
        self.assertFalse('5MNX' in results_1)
        self.assertTrue('5I1R' in results_1)
        self.assertFalse('5MON' in results_1)
        self.assertFalse('5LCB' in results_1)
        self.assertFalse('3J07' in results_1)


    def test9(self):
        pdb_9 = self.pdb.filter(experimentalMethods(experimentalMethods.SOLUTION_NMR))\
                        .filter(experimentalMethods(experimentalMethods.SOLUTION_SCATTERING))
        results_9 = pdb_9.keys().collect()

        self.assertFalse('2ONX' in results_9)
        self.assertFalse('5VLN' in results_9)
        self.assertFalse('5VAI' in results_9)
        self.assertFalse('5JXV' in results_9)
        self.assertFalse('5K7N' in results_9)
        self.assertFalse('3PDM' in results_9)
        self.assertFalse('5MNX' in results_9)
        self.assertTrue('5I1R' in results_9)
        self.assertFalse('5MON' in results_9)
        self.assertFalse('5LCB' in results_9)
        self.assertFalse('3J07' in results_9)


    def test10(self):
        pdb_10 = self.pdb.filter(experimentalMethods(experimentalMethods.SOLID_STATE_NMR))\
                         .filter(experimentalMethods(experimentalMethods.ELECTRON_MICROSCOPY))\
                         .filter(experimentalMethods(experimentalMethods.SOLUTION_SCATTERING))
        results_10 = pdb_10.keys().collect()

        self.assertFalse('2ONX' in results_10)
        self.assertFalse('5VLN' in results_10)
        self.assertFalse('5VAI' in results_10)
        self.assertFalse('5JXV' in results_10)
        self.assertFalse('5K7N' in results_10)
        self.assertFalse('3PDM' in results_10)
        self.assertFalse('5MNX' in results_10)
        self.assertFalse('5I1R' in results_10)
        self.assertFalse('5MON' in results_10)
        self.assertFalse('5LCB' in results_10)
        self.assertTrue('3J07' in results_10)


    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
