#!/usr/bin/env python
'''mmtfStructure.py

Decode msgpack unpacked data to mmtf structure

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import numpy as np
from mmtf.utils import decoder_utils
from mmtfPyspark.utils import mmtfDecoder


class MmtfStructure(object):
    model_counter = 0
    chain_counter = 0
    group_counter = 0
    atom_counter = 0

    def __init__(self, input_data):
        """Decodes a msgpack unpacked data to mmtf structure"""

        self.mmtf_version = mmtfDecoder.get_value(input_data, 'mmtfVersion', required=True)
        self.mmtf_producer = mmtfDecoder.get_value(input_data, 'mmtfProducer', required=True)
        self.unit_cell = mmtfDecoder.get_value(input_data, 'unitCell')
        self.space_group = mmtfDecoder.get_value(input_data, 'spaceGroup')
        self.structure_id = mmtfDecoder.get_value(input_data, 'structureId')
        self.title = mmtfDecoder.get_value(input_data, 'title')
        self.deposition_date = mmtfDecoder.get_value(input_data, 'depositionDate')
        self.release_date = mmtfDecoder.get_value(input_data, 'releaseDate')
        self.ncs_operator_list = mmtfDecoder.get_value(input_data, 'ncsOperatorList')
        self.bio_assembly = mmtfDecoder.get_value(input_data, 'bioAssemblyList')  # TODO naming inconsistency
        self.entity_list = mmtfDecoder.get_value(input_data, 'entityList')
        self.experimental_methods = mmtfDecoder.get_value(input_data, 'experimentalMethods')
        self.resolution = mmtfDecoder.get_value(input_data, 'resolution')
        self.r_free = mmtfDecoder.get_value(input_data, 'rFree')
        self.r_work = mmtfDecoder.get_value(input_data, 'rWork')
        self.num_bonds = mmtfDecoder.get_value(input_data, 'numBonds', required=True)
        self.num_atoms = mmtfDecoder.get_value(input_data, 'numAtoms', required=True)
        self.num_groups = mmtfDecoder.get_value(input_data, 'numGroups', required=True)
        self.num_chains = mmtfDecoder.get_value(input_data, 'numChains', required=True)
        self.num_models = mmtfDecoder.get_value(input_data, 'numModels', required=True)
        self.group_list = mmtfDecoder.get_values(input_data, 'groupList', required=True)
        self.bond_atom_list = mmtfDecoder.decode(input_data, 'bondAtomList')
        self.bond_order_list = mmtfDecoder.decode(input_data, 'bondOrderList')
        self.bondResonanceList = None  # TODO
        self.x_coord_list = mmtfDecoder.decode(input_data, 'xCoordList', required=True)
        self.y_coord_list = mmtfDecoder.decode(input_data, 'yCoordList', required=True)
        self.z_coord_list = mmtfDecoder.decode(input_data, 'zCoordList', required=True)
        self.b_factor_list = mmtfDecoder.decode(input_data, 'bFactorList')
        self.atom_id_list = mmtfDecoder.decode_n(input_data, 'atomIdList', self.num_atoms)
        self.alt_loc_list = mmtfDecoder.decode_n(input_data, 'altLocList', self.num_atoms)
        self.occupancy_list = mmtfDecoder.decode_n(input_data, 'occupancyList', self.num_atoms)
        self.group_id_list = mmtfDecoder.decode_n(input_data, 'groupIdList', self.num_groups, required=True)
        self.group_type_list = mmtfDecoder.decode(input_data, 'groupTypeList', required=True)
        self.sec_struct_list = mmtfDecoder.decode(input_data, 'secStructList')
        self.ins_code_list = mmtfDecoder.decode_n(input_data, "insCodeList", self.num_groups)
        self.sequence_index_list = mmtfDecoder.decode_n(input_data, "sequenceIndexList", self.num_atoms)
        self.chain_id_list = mmtfDecoder.decode(input_data, 'chainIdList', required=True)
        self.chain_name_list = mmtfDecoder.decode(input_data, 'chainNameList')
        self.groups_per_chain = mmtfDecoder.get_value(input_data, 'groupsPerChain', required=True)
        self.chains_per_model = mmtfDecoder.get_value(input_data, 'chainsPerModel', required=True)

