#!/usr/bin/env python

import unittest
from mmtfPyspark.interactions import *
from mmtfPyspark.utils import ColumnarStructure
from mmtfPyspark.io import mmtfReader
from pyspark.sql import SparkSession
import numpy as np

class ColumnarStructureTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("columnarStructure") \
                                 .getOrCreate()
        self.pdb = mmtfReader.download_mmtf_files(['1STP'])

        structure = self.pdb.values().first()
        self.cs = ColumnarStructure(structure, True)


    def test_get_x_coords(self):
        self.assertAlmostEqual(self.cs.get_x_coords()[20], 26.260,3)


    def test_get_elements(self):
        self.assertTrue(self.cs.get_elements()[20] == "C")


    def test_get_atom_names(self):
        self.assertTrue(self.cs.get_atom_names()[900] == "CG2")


    def test_get_group_names(self):
        self.assertTrue(self.cs.get_group_names()[900] == "VAL")


    def test_is_polymer(self):
        self.assertTrue(self.cs.is_polymer()[100] == True)
        self.assertTrue(self.cs.is_polymer()[901] == False)
        self.assertTrue(self.cs.is_polymer()[917] == False)


    def test_get_group_numbers(self):
        self.assertTrue(self.cs.get_group_numbers()[877])


    def test_get_chain_ids(self):
        self.assertTrue(self.cs.get_chain_ids()[100] == 'A')
        self.assertTrue(self.cs.get_chain_ids()[901] == 'B')
        self.assertTrue(self.cs.get_chain_ids()[917] == 'C')


    def test_get_chem_comp_types(self):
        self.assertTrue(self.cs.get_chem_comp_types()[100] == 'PEPTIDE LINKING')
        self.assertTrue(self.cs.get_chem_comp_types()[901] == 'NON-POLYMER')
        self.assertTrue(self.cs.get_chem_comp_types()[917] == 'NON-POLYMER')


    def test_get_entity_types(self):
        self.assertTrue(self.cs.get_entity_types()[100] == 'PRO')
        self.assertTrue(self.cs.get_entity_types()[901] == 'LGO')
        self.assertTrue(self.cs.get_entity_types()[917] == 'WAT')


    def tearDown(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()
