#!/usr/bin/env python

import unittest
from mmtfPyspark.interactions import *
from mmtfPyspark.utils import ColumnarStructure
from mmtfPyspark.io import MmtfReader
from pyspark import SparkConf, SparkContext
import numpy as np
from math import isclose

class testCoordiateGeometry(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster("local[*]").setAppName('coordinateGeometry')
        self.sc = SparkContext(conf=conf)

        self.pdb = MmtfReader.download_mmtf_files(['5Y20'], self.sc)


    def get_coords(self, cs, index):
        return np.array([cs.get_x_coords()[index],\
                         cs.get_y_coords()[index],\
                         cs.get_z_coords()[index]])


    def test1(self):

        structure = self.pdb.values().first()
        cs = ColumnarStructure(structure, True)

        center = self.get_coords(cs, 459)   # ZN A.101.ZN
        neighbors = []
        neighbors.append(self.get_coords(cs, 28))   # CYS A.7.SG
        neighbors.append(self.get_coords(cs, 44))   # CYS A.10.SG
        neighbors.append(self.get_coords(cs, 223))   # HIS A.31.ND1
        neighbors.append(self.get_coords(cs, 245))   # CYS A.34.SG
        neighbors.append(self.get_coords(cs, 45))   # CYS A.10.N
        neighbors.append(self.get_coords(cs, 220))   # HIS A.31.O

        geom = CoordinateGeometry(center, neighbors)

        self.assertTrue(isclose(geom.q3(), 0.9730115379131878, abs_tol = 1e-4))
        self.assertTrue(isclose(geom.q4(), 0.9691494056145086, abs_tol = 1e-4))
        self.assertTrue(isclose(geom.q5(), 0.5126001729084566, abs_tol = 1e-4))
        self.assertTrue(isclose(geom.q6(), 0.2723305441457363, abs_tol = 1e-4))


    def tearDown(self):
        self.sc.stop()

if __name__ == '__main__':
    unittest.main()
