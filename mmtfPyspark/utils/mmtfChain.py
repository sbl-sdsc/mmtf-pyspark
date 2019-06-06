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

        indices = np.where(self.structure.chain_name_list == self.chain_name)
        if indices[0].size == 0:
            raise ValueError("Structure " + self.structure.structure_id + " does not contain chain: " + self.chain_name)

        # find start and end of the first polymer chain
        for i in indices[0]:
            ind = structure.entityChainIndex[i]
            if self.structure.entity_list[ind]['type'] == 'polymer':
                self.start = self.structure.chainToAtomIndices[i]
                self.end = self.structure.chainToAtomIndices[i+1]
                break

    @property
    def atom_id_list(self):
        """Return atom id list"""
        return self.structure.atom_id_list[self.start:self.end]

    @property
    def x_coord_list(self):
        """Return atom id list"""
        return self.structure.x_coord_list[self.start:self.end]



