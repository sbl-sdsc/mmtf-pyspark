#!/usr/bin/env python
'''mmtfChain.py

Decode msgpack unpacked data to mmtf chain

'''
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__version__ = "0.4.0"
__status__ = "Experimental"

from numpy import np
from mmtfPyspark.utils import MmtfStructure


class MmtfChain(MmtfStructure):

    def __init__(self, input_data, chain_name):
        """Decodes a msgpack unpacked data to mmtf structure"""
        MmtfStructure.__init__(self, input_data)
        if chain_name not in self.chain_name_list:
            raise ValueError("Structure" + self.structure_id + " does not chain: " + chain_name)
        self.chain_name = chain_name

