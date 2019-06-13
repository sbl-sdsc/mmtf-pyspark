#!/usr/bin/env python
'''mmtfModel.py

Extracts the specified model from a structure

'''
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__version__ = "0.4.0"
__status__ = "Experimental"

import numpy as np
import pandas as pd
from mmtfPyspark.utils import MmtfChain


class MmtfModel(object):

    def __init__(self, structure, model_number):
        """Extracts the specified model from a structure"""
        self.structure = structure
        self.model_number = model_number
        self.start = None
        self.end = None

        if model_number >= structure.num_models:
            raise ValueError("Structure " + structure.structure_id + " does not contain model: " + model_number)

        # find start and end of the first polymer chain
        self.start = structure.modelToAtomIndices[model_number]
        self.end = structure.modelToAtomIndices[model_number+1]
        self.num_atoms = self.end - self.start
        self.num_groups = structure.modelToGroupIndices[model_number+1] - structure.modelToGroupIndices[model_number]
        self.num_chains = structure.modelToChainIndices[model_number+1] - structure.modelToChainIndices[model_number]
        self.num_models = 1

        self.entityChainIndex = structure.entityChainIndex
        self.chain_name_list = self.structure.chain_name_list

        self.mmtf_version = structure.mmtf_version
        self.mmtf_producer = structure.mmtf_producer
        self.unit_cell = structure.unit_cell
        self.space_group = structure.space_group
        self.structure_id = structure.structure_id + "-m" + str(model_number)
        self.title = structure.title
        self.deposition_date = structure.deposition_date
        self.release_date = structure.release_date
        self.ncs_operator_list = structure.ncs_operator_list
        # TODO
        self.bio_assembly = None
        self.entity_list = structure.entity_list
        self.experimental_methods = structure.experimental_methods
        self.resolution = structure.resolution
        self.r_free = structure.r_free
        self.r_work = structure.r_work

        # calculated indices
        self.modelToAtomIndices = None
        self.modelToGroupIndices = None
        self.modelToChainIndices = None
        self.groupToAtomIndices = structure.groupToAtomIndices[
                                  structure.modelToGroupIndices[model_number]:
                                  structure.modelToGroupIndices[model_number+1]
        ]
        self.chainToAtomIndices = structure.chainToAtomIndices[
                                  structure.modelToChainIndices[model_number]:
                                  structure.modelToChainIndices[model_number+1]]

        self.chainToGroupIndices = structure.chainToGroupIndices[
                                   structure.modelToChainIndices[model_number]:
                                   structure.modelToChainIndices[model_number + 1]]
        # dataframes
        self.df = None

    @property
    def atom_id_list(self):
        """Return atom id list"""
        return self.structure.atom_id_list[self.start:self.end]

    @property
    def x_coord_list(self):
        """Return x coordinates"""
        return self.structure.x_coord_list[self.start:self.end]

    @property
    def y_coord_list(self):
        """Return y coordinates"""
        return self.structure.y_coord_list[self.start:self.end]

    @property
    def z_coord_list(self):
        """Return z coordinates"""
        return self.structure.z_coord_list[self.start:self.end]

    @property
    def coords(self):
        """Return 3xn coordinate array"""
        return np.column_stack((self.x_coord_list, self.y_coord_list, self.z_coord_list))

    @property
    def b_factor_list(self):
        """Return b factors"""
        if self.structure.b_factor_list is None:
            return None
        return self.structure.b_factor_list[self.start:self.end]

    @property
    def occupancy_list(self):
        """Return occupancies"""
        if self.structure.occupancy_list is None:
            return None
        return self.structure.occupancy_list[self.start:self.end]

    @property
    def alt_loc_list(self):
        """Return alternative location codes"""
        if self.structure.alt_loc_list is None:
            return None
        return self.structure.alt_loc_list[self.start:self.end]

    # calculated properties
    @property
    def chain_names(self):
        """Return chain names"""
        return self.structure.chain_names[self.start:self.end]

    @property
    def chain_ids(self):
        """Return chain ids"""
        return self.structure.chain_ids[self.start:self.end]

    @property
    def group_numbers(self):
        """Return group numbers"""
        return self.structure.group_numbers[self.start:self.end]

    @property
    def group_names(self):
        """Return group names"""
        return self.structure.group_names[self.start:self.end]

    @property
    def atom_names(self):
        """Return group names"""
        return self.structure.atom_names[self.start:self.end]

    @property
    def elements(self):
        """Return group names"""
        return self.structure.elements[self.start:self.end]

    @property
    def chem_comp_types(self):
        """Return group names"""
        return self.structure.chem_comp_types[self.start:self.end]

    @property
    def polymer(self):
        """Return polymer"""
        return self.structure.polymer[self.start:self.end]

    @property
    def entity_indices(self):
        """Return entity indices"""
        return self.structure.entity_indices[self.start:self.end]

    @property
    def sequence_positions(self):
        """Return sequence_positions"""
        return self.structure.sequence_positions[self.start:self.end]

    def get_chain(self, chain_name):
        """Return specified polymer chain"""
        return MmtfChain(self, chain_name)

    def get_chains(self):
        """Return polymer chains"""
        chains = []
        for chain_name in set(self.structure.chain_name_list):
            chains.append(MmtfChain(self, chain_name))

        return chains

    def to_pandas(self, multi_index=False):
        if self.df is None:
            self.df = pd.DataFrame({'chain_name': self.get_chain_names(),
                                    'chain_id': self.get_chain_ids(),
                                    'group_number': self.get_group_numbers(),
                                    'group_name': self.get_group_names(),
                                    'atom_name': self.get_atom_names(),
                                    'altloc': self.get_alt_loc_list(),
                                    'x': self.get_x_coords(),
                                    'y': self.get_y_coords(),
                                    'z': self.get_z_coords(),
                                    'o': self.get_occupancies(),
                                    'b': self.get_b_factors(),
                                    'element': self.get_elements(),
                                    'polymer': self.is_polymer(),
                                    #                               'entity': self.get_entity_indices(),
                                    #                                   'seq_index': self.get_sequence_positions()
                                    })
            if multi_index:
                self.df.set_index(['chain_name', 'group_number', 'group_name', 'atom_name', 'altloc'], inplace=True)

        return self.df







