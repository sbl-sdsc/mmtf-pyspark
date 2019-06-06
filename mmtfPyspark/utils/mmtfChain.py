#!/usr/bin/env python
'''mmtfChain.py

Decode msgpack unpacked data to mmtf chain

'''
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__version__ = "0.4.0"
__status__ = "Experimental"

import numpy as np
from mmtfPyspark.utils import MmtfStructure
from mmtfPyspark.utils import mmtfDecoder


class MmtfChain(MmtfStructure):

    def __init__(self, input_data, chain_name):
        """Decodes a msgpack unpacked data to mmtf structure"""
        self.start = None
        self.end = None

        MmtfStructure.__init__(self, input_data)
        indices = np.where(self.chain_name_list == chain_name)
        if indices[0].size == 0:
            raise ValueError("Structure " + self.structure_id + " does not contain chain: " + chain_name)

        # find start and end of polymer chain
        for i in indices[0]:
            ind = self.entityChainIndex[i]
            if self.entity_list[ind]['type'] == 'polymer':
                self.start = self.chainToAtomIndices[i]
                self.end = self.chainToAtomIndices[i+1]

        self.chain_name = chain_name

    @property
    def atom_id_list(self):
        """Return atom id list"""
        if self._atom_id_list is not None:
            return self._atom_id_list[self.start:self.end]
        elif 'atomIdList' in self.input_data:
            self._atom_id_list = mmtfDecoder.decode(self.input_data, 'atomIdList')
            return self._atom_id_list[self.start:self.end]
        else:
            return None


