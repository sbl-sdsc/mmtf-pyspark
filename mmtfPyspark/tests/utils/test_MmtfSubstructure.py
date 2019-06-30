#!/usr/bin/env python
'''

Authorship information:
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__status__ = "Warning"
'''

import unittest
import numpy as np
from pyspark.sql import SparkSession
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.utils import MmtfSubstructure


class TestMmtfSubstructure(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[1]") \
                                 .appName("TestMmtfSubstructure") \
                                 .getOrCreate()

    def test_4HHB_polychain(self):
        print('test_4HHB_polychain')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        chain = MmtfSubstructure(structure, 'A', chain_names=['A'], entity_types=['polymer'])
        self.assertEqual(1069, chain.num_atoms)
        self.assertEqual(141, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)
        self.assertEqual(np.testing.assert_allclose([6.204, 6.913, 8.504],
                                                    chain.x_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([16.869, 17.759, 17.378],
                                                    chain.y_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([4.854, 4.607, 4.797],
                                                    chain.z_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([49.05, 43.14, 24.80],
                                                    chain.b_factor_list[0:3], atol=0.01), None)
        self.assertEqual(np.testing.assert_allclose([1.0, 1.0, 1.0],
                                                    chain.occupancy_list[0:3], atol=0.01), None)
        self.assertListEqual([1, 2, 3], chain.atom_id_list[0:3].tolist())
        self.assertListEqual(['', '', ''], chain.alt_loc_list[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], chain.chain_names[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], chain.chain_ids[0:3].tolist())
        self.assertListEqual(['1', '1', '1'], chain.group_numbers[0:3].tolist())
        self.assertListEqual(['VAL', 'VAL', 'VAL'], chain.group_names[0:3].tolist())
        self.assertListEqual(['N', 'CA', 'C'], chain.atom_names[0:3].tolist())
        self.assertListEqual(['N', 'C', 'C'], chain.elements[0:3].tolist())
        self.assertListEqual(['L-PEPTIDE LINKING', 'L-PEPTIDE LINKING'], chain.chem_comp_types[0:2].tolist())
        self.assertListEqual([True, True, True], chain.polymer[0:3].tolist())
        self.assertListEqual([0, 0, 0], chain.entity_indices[0:3].tolist())
        self.assertListEqual([0, 0, 0], chain.sequence_positions[0:3].tolist())

        chain = MmtfSubstructure(structure, 'B', chain_names=['B'], entity_types=['polymer'])
        self.assertEqual(1123, chain.num_atoms)
        self.assertEqual(146, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)
        self.assertEqual(np.testing.assert_allclose([9.223, 8.694, 9.668],
                                                    chain.x_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([-20.614, -20.026, -21.068],
                                                    chain.y_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([1.365, -0.123, -1.645],
                                                    chain.z_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([46.08, 70.96, 69.74],
                                                    chain.b_factor_list[0:3], atol=0.01), None)
        self.assertEqual(np.testing.assert_allclose([1.0, 1.0, 1.0],
                                                    chain.occupancy_list[0:3], atol=0.01), None)
        self.assertListEqual([1070, 1071, 1072], chain.atom_id_list[0:3].tolist())
        self.assertListEqual(['', '', ''], chain.alt_loc_list[0:3].tolist())
        self.assertListEqual(['B', 'B', 'B'], chain.chain_names[0:3].tolist())
        self.assertListEqual(['B', 'B', 'B'], chain.chain_ids[0:3].tolist())
        self.assertListEqual(['1', '1', '1'], chain.group_numbers[0:3].tolist())
        self.assertListEqual(['VAL', 'VAL', 'VAL'], chain.group_names[0:3].tolist())
        self.assertListEqual(['N', 'CA', 'C'], chain.atom_names[0:3].tolist())
        self.assertListEqual(['N', 'C', 'C'], chain.elements[0:3].tolist())
        self.assertListEqual(['L-PEPTIDE LINKING', 'L-PEPTIDE LINKING'], chain.chem_comp_types[0:2].tolist())
        self.assertListEqual([True, True, True], chain.polymer[0:3].tolist())
        self.assertListEqual([1, 1, 1], chain.entity_indices[0:3].tolist())
        self.assertListEqual([0, 0, 0], chain.sequence_positions[0:3].tolist())

    def test_4HHB_polychains(self):
        print('test_4HHB_polychains')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        chain = MmtfSubstructure(structure, 'A+B', chain_names=['A', 'B'], entity_types=['polymer'])
        self.assertEqual(1069+1123, chain.num_atoms)
        self.assertEqual(141+146, chain.num_groups)
        self.assertEqual(1+1, chain.num_chains)
        self.assertEqual(1, chain.num_models)

    def test_4HHB_chain_ids(self):
        print('test_4HHB_chain_ids')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        chain = MmtfSubstructure(structure, 'A', chain_ids=['A'])
        self.assertEqual(1069, chain.num_atoms)
        np.set_printoptions(threshold=np.inf)
        print(chain.group_serial)
        self.assertEqual(141, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)
        self.assertEqual(np.testing.assert_allclose([6.204, 6.913, 8.504],
                                                    chain.x_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([16.869, 17.759, 17.378],
                                                    chain.y_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([4.854, 4.607, 4.797],
                                                    chain.z_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([49.05, 43.14, 24.80],
                                                    chain.b_factor_list[0:3], atol=0.01), None)
        self.assertEqual(np.testing.assert_allclose([1.0, 1.0, 1.0],
                                                    chain.occupancy_list[0:3], atol=0.01), None)
        self.assertListEqual([1, 2, 3], chain.atom_id_list[0:3].tolist())
        self.assertListEqual(['', '', ''], chain.alt_loc_list[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], chain.chain_names[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], chain.chain_ids[0:3].tolist())
        self.assertListEqual(['1', '1', '1'], chain.group_numbers[0:3].tolist())
        self.assertListEqual(['VAL', 'VAL', 'VAL'], chain.group_names[0:3].tolist())
        self.assertListEqual(['N', 'CA', 'C'], chain.atom_names[0:3].tolist())
        self.assertListEqual(['N', 'C', 'C'], chain.elements[0:3].tolist())
        self.assertListEqual(['L-PEPTIDE LINKING', 'L-PEPTIDE LINKING'], chain.chem_comp_types[0:2].tolist())
        self.assertListEqual([True, True, True], chain.polymer[0:3].tolist())
        self.assertListEqual([0, 0, 0], chain.entity_indices[0:3].tolist())
        self.assertListEqual([0, 0, 0], chain.sequence_positions[0:3].tolist())

        chain = MmtfSubstructure(structure, 'B', chain_names=['B'], entity_types=['polymer'])
        self.assertEqual(1123, chain.num_atoms)
        self.assertEqual(146, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)
        self.assertEqual(np.testing.assert_allclose([9.223, 8.694, 9.668],
                                                    chain.x_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([-20.614, -20.026, -21.068],
                                                    chain.y_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([1.365, -0.123, -1.645],
                                                    chain.z_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([46.08, 70.96, 69.74],
                                                    chain.b_factor_list[0:3], atol=0.01), None)
        self.assertEqual(np.testing.assert_allclose([1.0, 1.0, 1.0],
                                                    chain.occupancy_list[0:3], atol=0.01), None)
        self.assertListEqual([1070, 1071, 1072], chain.atom_id_list[0:3].tolist())
        self.assertListEqual(['', '', ''], chain.alt_loc_list[0:3].tolist())
        self.assertListEqual(['B', 'B', 'B'], chain.chain_names[0:3].tolist())
        self.assertListEqual(['B', 'B', 'B'], chain.chain_ids[0:3].tolist())
        self.assertListEqual(['1', '1', '1'], chain.group_numbers[0:3].tolist())
        self.assertListEqual(['VAL', 'VAL', 'VAL'], chain.group_names[0:3].tolist())
        self.assertListEqual(['N', 'CA', 'C'], chain.atom_names[0:3].tolist())
        self.assertListEqual(['N', 'C', 'C'], chain.elements[0:3].tolist())
        self.assertListEqual(['L-PEPTIDE LINKING', 'L-PEPTIDE LINKING'], chain.chem_comp_types[0:2].tolist())
        self.assertListEqual([True, True, True], chain.polymer[0:3].tolist())
        self.assertListEqual([1, 1, 1], chain.entity_indices[0:3].tolist())
        self.assertListEqual([0, 0, 0], chain.sequence_positions[0:3].tolist())

    def test_4HHB_group_names(self):
        print('test_4HHB_chain_ids')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        chain = MmtfSubstructure(structure, 'HEM', chain_names=['A'], group_names=['HEM'])
        self.assertEqual(43, chain.num_atoms)
        self.assertEqual(1, chain.num_groups)
        self.assertEqual(1, chain.num_chains)
        self.assertEqual(1, chain.num_models)

        chain = MmtfSubstructure(structure, 'HEM', group_names=['HEM'])
        self.assertEqual(43*4, chain.num_atoms)
        self.assertEqual(1*4, chain.num_groups)
        self.assertEqual(1*4, chain.num_chains)
        self.assertEqual(1, chain.num_models)

    # def test_4HHB_chains(self):
    #     print('test_4HHB_chains')
    #     path = '../../../resources/files/'
    #     pdb = mmtfReader.read_mmtf_files(path)
    #     pdb = pdb.filter(lambda t: t[0] == '4HHB')
    #     structure = pdb.values().first()
    #     chains = structure.get_chains()
    #     self.assertEqual(4, len(chains))

    # def test_4HHB_chains_first_model(self):
    #     print('test_4HHB_chains_first_model')
    #     path = '../../../resources/files/'
    #     pdb = mmtfReader.read_mmtf_files(path, first_model=True)
    #     pdb = pdb.filter(lambda t: t[0] == '4HHB')
    #     structure = pdb.values().first()
    #     chains = structure.get_chains()
    #     self.assertEqual(4, len(chains))
    #
    # def test_1J6T_chains_first_model(self):
    #     print('test_1J6T_chains_first_model')
    #     path = '../../../resources/files/'
    #     pdb = mmtfReader.read_mmtf_files(path, first_model=True)
    #     pdb = pdb.filter(lambda t: t[0] == '1J6T')
    #     structure = pdb.values().first()
    #     chains = structure.get_chains()
    #     self.assertEqual(2, len(chains))

    # def test_4HHB_multiple_chains(self):
    #     print('test_4HHB_multiple_chains')
    #     path = '../../../resources/files/'
    #     pdb = mmtfReader.read_mmtf_files(path)
    #     pdb = pdb.filter(lambda t: t[0] == '4HHB')
    #     structure = pdb.values().first()
    #     chain_list = ['A']
    #     chains = structure.get_multiple_chains(chain_list)
    #     self.assertEqual(1168, chains.x_coord_list.size)
    #     chain_list = ['A', 'B']
    #     chains = structure.get_multiple_chains(chain_list)
    #     self.assertEqual(2392, chains.x_coord_list.shape[0])
    #     chain_list = ['A', 'B', 'C']
    #     chains = structure.get_multiple_chains(chain_list)
    #     self.assertEqual(3563, chains.x_coord_list.shape[0])
    #     chain_list = ['A', 'B', 'C', 'D']
    #     chains = structure.get_multiple_chains(chain_list)
    #     self.assertEqual(4779, chains.x_coord_list.shape[0])

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
