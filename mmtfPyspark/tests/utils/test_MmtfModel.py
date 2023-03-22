#!/usr/bin/env python
'''

Authorship information:
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__status__ = "Warning"
'''
import os
import unittest
import numpy as np
from pyspark.sql import SparkSession
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.utils import MmtfSubstructure

FIXTURE_DIR = os.path.dirname(os.path.realpath(__file__))

class TestMmtfModel(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("TestMmtfModel") \
                                 .getOrCreate()

    def test_1J6T_model(self):
        print('test_1J6T_model')
        path = FIXTURE_DIR + '/../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '1J6T')
        structure = pdb.values().first()
        model = structure.get_model(0)
        self.assertEqual(3555, model.num_atoms)
        self.assertEqual(144+85, model.num_groups)
        self.assertEqual(2, model.num_chains)
        self.assertEqual(1, model.num_models)
        model = structure.get_model(1)
        self.assertEqual(3559, model.num_atoms)
        self.assertEqual(144+85+1, model.num_groups)
        self.assertEqual(3, model.num_chains)
        self.assertEqual(1, model.num_models)
        model = structure.get_model(2)
        self.assertEqual(3559, model.num_atoms)
        self.assertEqual(144+85+1, model.num_groups)
        self.assertEqual(3, model.num_chains)
        self.assertEqual(1, model.num_models)

    def test_1J6T_model_first_model(self):
        print('test_1J6T_model_first_model')
        path = FIXTURE_DIR + '/../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path, first_model=True)
        pdb = pdb.filter(lambda t: t[0] == '1J6T')
        structure = pdb.values().first()
        model = structure.get_model(0)
        self.assertEqual(3555, model.num_atoms)
        self.assertEqual(144+85, model.num_groups)
        self.assertEqual(2, model.num_chains)
        self.assertEqual(1, model.num_models)

    def test_1J6T_model_chains(self):
        print('test_1J6T_model_chains')
        path = FIXTURE_DIR + '/../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '1J6T')
        structure = pdb.values().first()
        model = structure.get_model(0)
        self.assertEqual(3555, model.num_atoms)
        self.assertEqual(144+85, model.num_groups)
        self.assertEqual(2, model.num_chains)
        self.assertEqual(1, model.num_models)
        chain = model.get_chain('A')
        self.assertEqual(2266, chain.num_atoms)
        self.assertEqual(144, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)

        model = structure.get_model(1)
        self.assertEqual(3559, model.num_atoms)
        self.assertEqual(144+85+1, model.num_groups)
        self.assertEqual(3, model.num_chains)
        self.assertEqual(1, model.num_models)
        chain = model.get_chain('A')
        self.assertEqual(2266, chain.num_atoms)
        self.assertEqual(144, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)
        chain = model.get_chain('B')
        self.assertEqual(1289, chain.num_atoms)
        self.assertEqual(85, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)

        model = structure.get_model(2)
        self.assertEqual(3559, model.num_atoms)
        self.assertEqual(144 + 85 + 1, model.num_groups)
        self.assertEqual(3, model.num_chains)
        self.assertEqual(1, model.num_models)
        chain = model.get_chain('A')
        self.assertEqual(2266, chain.num_atoms)
        self.assertEqual(144, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)
        chain = model.get_chain('B')
        self.assertEqual(1289, chain.num_atoms)
        self.assertEqual(85, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)

    def test_1J6T_models(self):
        print('test_1J6T_modelS')
        path = FIXTURE_DIR + '/../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '1J6T')
        structure = pdb.values().first()
        models = structure.get_models()
        self.assertEqual(3, len(models))

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
