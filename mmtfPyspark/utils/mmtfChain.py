#!/usr/bin/env python
'''mmtfChain.py

Decode msgpack unpacked data to mmtf chain

'''
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__version__ = "0.4.0"
__status__ = "Experimental"

import numpy as np


class MmtfChain(object):

    def __init__(self, structure, chain_name):
        """Decodes a msgpack unpacked data to mmtf structure"""
        self.structure = structure
        self.chain_name = chain_name
        self.start = None
        self.end = None
        self.num_atoms = 0
        self.num_groups = 0
        self.num_chains = 1
        self.num_models = 1

        indices = np.where(structure.chain_name_list == self.chain_name)
        if indices[0].size == 0:
            raise ValueError("Structure " + structure.structure_id + " does not contain chain: " + self.chain_name)

        # find start and end of the first polymer chain
        for i in indices[0]:
            ind = structure.entityChainIndex[i]
            if structure.entity_list[ind]['type'] == 'polymer':
                self.start = structure.chainToAtomIndices[i]
                self.end = structure.chainToAtomIndices[i+1]
                self.num_atoms = self.end - self.start
                self.num_groups = structure.chainToGroupIndices[i+1] - structure.chainToGroupIndices[i]
                break

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








