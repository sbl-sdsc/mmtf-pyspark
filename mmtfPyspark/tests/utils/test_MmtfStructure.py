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


class TestMmtfStructure(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[1]") \
                                 .appName("TestMmtfStructure") \
                                 .getOrCreate()

    def test_4HHB_structure(self):
        print('test_4HHB_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()

        # Check metadata
        self.assertEqual('1.0.0', structure.mmtf_version)
        self.assertEqual('UNKNOWN', structure.mmtf_producer)
        self.assertEqual(np.testing.assert_allclose([63.150, 83.590, 53.800, 90.00, 99.34, 90.00],
                                                    structure.unit_cell, atol=0.001), None)
        self.assertEqual(structure.space_group, 'P 1 21 1')
        self.assertEqual(4779, structure.num_atoms)
        self.assertEqual(801, structure.num_groups)
        self.assertEqual(14, structure.num_chains)
        self.assertEqual(1, structure.num_models)

        # Check entity info
        ids = structure.entities_to_pandas()['chain_ids'].tolist()
        self.assertEqual(5, len(ids))
        self.assertListEqual(['A', 'C'], ids[0])  # Hemoglobin alpha
        self.assertListEqual(['B', 'D'], ids[1])  # Hemoglobin beta
        self.assertListEqual(['E', 'G', 'H', 'J'], ids[2])  # Heme
        self.assertListEqual(['F', 'I'], ids[3])  # PO4
        self.assertListEqual(['K', 'L', 'M', 'N'], ids[4])  # HOH

        # Check atom info
        self.assertEqual(np.testing.assert_allclose([6.204, 6.913, 8.504],
                                                    structure.x_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([16.869, 17.759, 17.378],
                                                    structure.y_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([4.854, 4.607, 4.797],
                                                    structure.z_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([49.05, 43.14, 24.80],
                                                    structure.b_factor_list[0:3], atol=0.01), None)
        self.assertEqual(np.testing.assert_allclose([1.0, 1.0, 1.0],
                                                    structure.occupancy_list[0:3], atol=0.01), None)
        self.assertListEqual([1, 2, 3], structure.atom_id_list[0:3].tolist())
        self.assertListEqual(['', '', ''], structure.alt_loc_list[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], structure.chain_names[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], structure.chain_ids[0:3].tolist())
        self.assertListEqual(['1', '1', '1'], structure.group_numbers[0:3].tolist())
        self.assertListEqual(['VAL', 'VAL', 'VAL'], structure.group_names[0:3].tolist())
        self.assertListEqual(['N', 'CA', 'C'], structure.atom_names[0:3].tolist())
        self.assertListEqual(['N', 'C', 'C'], structure.elements[0:3].tolist())
        self.assertListEqual(['L-PEPTIDE LINKING', 'L-PEPTIDE LINKING'], structure.chem_comp_types[0:2].tolist())
        self.assertListEqual([True, True, True], structure.polymer[0:3].tolist())
        self.assertListEqual([0, 0, 0], structure.entity_indices[0:3].tolist())  # hemoglobin alpha
        self.assertListEqual([1, 1, 1], structure.entity_indices[1069:1072].tolist()) # hemoglobin beta
        self.assertListEqual([4, 4, 4], structure.entity_indices[4776:4779].tolist()) # water
        self.assertListEqual([0, 0, 0], structure.sequence_positions[0:3].tolist())

    def test_4HHB_structure_first_model(self):
        print('test_4HHB_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path, first_model=True)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        self.assertEqual('1.0.0', structure.mmtf_version)
        self.assertEqual('UNKNOWN', structure.mmtf_producer)
        self.assertEqual(np.testing.assert_allclose([63.150, 83.590, 53.800, 90.00, 99.34, 90.00],
                                                    structure.unit_cell, atol=0.001), None)
        self.assertEqual(structure.space_group, 'P 1 21 1')
        self.assertEqual(4779, structure.num_atoms)
        self.assertEqual(801, structure.num_groups)
        self.assertEqual(14, structure.num_chains)
        self.assertEqual(1, structure.num_models)
        self.assertEqual(np.testing.assert_allclose([6.204, 6.913, 8.504],
                                                    structure.x_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([16.869, 17.759, 17.378],
                                                    structure.y_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([4.854, 4.607, 4.797],
                                                    structure.z_coord_list[0:3], atol=0.001), None)
        self.assertEqual(np.testing.assert_allclose([49.05, 43.14, 24.80],
                                                    structure.b_factor_list[0:3], atol=0.01), None)
        self.assertEqual(np.testing.assert_allclose([1.0, 1.0, 1.0],
                                                    structure.occupancy_list[0:3], atol=0.01), None)
        self.assertListEqual([1, 2, 3], structure.atom_id_list[0:3].tolist())
        self.assertListEqual(['', '', ''], structure.alt_loc_list[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], structure.chain_names[0:3].tolist())
        self.assertListEqual(['A', 'A', 'A'], structure.chain_ids[0:3].tolist())
        self.assertListEqual(['1', '1', '1'], structure.group_numbers[0:3].tolist())
        self.assertListEqual(['VAL', 'VAL', 'VAL'], structure.group_names[0:3].tolist())
        self.assertListEqual(['N', 'CA', 'C'], structure.atom_names[0:3].tolist())
        self.assertListEqual(['N', 'C', 'C'], structure.elements[0:3].tolist())
        self.assertListEqual(['L-PEPTIDE LINKING', 'L-PEPTIDE LINKING'], structure.chem_comp_types[0:2].tolist())
        self.assertListEqual([True, True, True], structure.polymer[0:3].tolist())
        self.assertListEqual([0, 0, 0], structure.entity_indices[0:3].tolist())  # hemoglobin alpha
        self.assertListEqual([1, 1, 1], structure.entity_indices[1069:1072].tolist()) # hemoglobin beta
        self.assertListEqual([4, 4, 4], structure.entity_indices[4776:4779].tolist()) # water
        self.assertListEqual([0, 0, 0], structure.sequence_positions[0:3].tolist())

    def test_1J6T_structure(self):
        print('test_1J6T_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '1J6T')
        structure = pdb.values().first()

        # Check entity info
        #self.assertEqual(3, len(structure.entity_list))
        self.assertEqual(2, len(structure.entity_list))  # MMTF file only contains entities for first model
        ids = structure.entities_to_pandas()['chain_ids'].tolist()
        self.assertEqual(2, len(ids))
        self.assertListEqual(['A'], ids[0])
        self.assertListEqual(['B'], ids[1])
        # self.assertListEqual(['C'], ids[2])  # PO3 missing in MMTF, present in model 2 and 3

        self.assertEqual(3555+3559+3559, structure.num_atoms)
        self.assertEqual(144+85+144+1+85+144+85+1, structure.num_groups)
        self.assertEqual(2+3+3, structure.num_chains)
        self.assertEqual(3, structure.num_models)

    def test_4HHB_pandas(self):
        print('test_4HHB_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        df = structure.to_pandas()
        print('columns', df.columns)
        self.assertEqual((4779, 13), df.shape)

    def test_4HHB_pandas_add_cols(self):
        print('test_4HHB_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        df = structure.to_pandas(add_cols=['sequence_position', 'chem_comp_type'])
        print('columns', df.columns)
        self.assertEqual((4779, 15), df.shape)

    def test_1J6T_structure_first_model(self):
        print('test_1J6T_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path, first_model=True)
        pdb = pdb.filter(lambda t: t[0] == '1J6T')
        structure = pdb.values().first()

        # Check entity info
        self.assertEqual(2, len(structure.entity_list))
        ids = structure.entities_to_pandas()['chain_ids'].tolist()
        self.assertEqual(2, len(ids))  # first model has only two entities (2nd, 3rd model have 3: +PO4)
        self.assertListEqual(['A'], ids[0])
        self.assertListEqual(['B'], ids[1])

        self.assertEqual(3555, structure.num_atoms)
        self.assertEqual(144+85, structure.num_groups)
        self.assertEqual(2, structure.num_chains)
        self.assertEqual(1, structure.num_models)

    def test_4HHB_pandas(self):
        print('test_4HHB_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        df = structure.to_pandas()
        self.assertEqual((4779, 13), df.shape)

    def test_4HHB_pandas_add_cols(self):
        print('test_4HHB_structure')
        path = '../../../resources/files/'
        pdb = mmtfReader.read_mmtf_files(path)
        pdb = pdb.filter(lambda t: t[0] == '4HHB')
        structure = pdb.values().first()
        df = structure.to_pandas(add_cols=['sequence_position', 'chem_comp_type'])
        self.assertEqual((4779, 15), df.shape)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
