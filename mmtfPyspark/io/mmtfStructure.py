#!/usr/bin/env python
'''
mmtfStructure.py: Decode msgpack unpacked data to mmtf structure

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

import numpy as np
import time
import struct
from mmtf.utils import decoder_utils
from mmtfPyspark.utils import mmtfDecoder


class mmtfStructure(object):
    model_counter = 0
    chain_counter = 0
    group_counter = 0
    atom_counter = 0

    def __init__(self, input_data):
        """
        Decodes a msgpack unpacked data to mmtf structure
        """

        # Variables that are not in all mmtf files
        if b"bFactorList" in input_data:
            int_array = np.frombuffer(input_data[b'bFactorList'][12:],'>i2')
            decode_num = np.frombuffer(input_data[b'bFactorList'][8:12],'>i')
            self.b_factor_list = mmtfDecoder.recursive_index_decode(int_array, decode_num)
        else:
            self.b_factor_list = []
        if b'resolution' in input_data:
            self.resolution = input_data[b'resolution']
        else:
            self.resolution = None
        if b"rFree" in input_data:
            self.r_free = input_data[b"rFree"]
        else:
            self.r_free = None
        if b"rWork" in input_data:
            self.r_work = input_data[b"rWork"]
        else:
            self.r_work = None
        if b"bioAssemblyList" in input_data:
            self.bio_assembly = input_data[b"bioAssemblyList"]
        else:
            self.bio_assembly = []
        if b"unitCell" in input_data:
            self.unit_cell = input_data[b"unitCell"]
        else:
            self.unit_cell = None
        if b"releaseDate" in input_data:
            self.release_date = input_data[b"releaseDate"].decode()
        else:
            self.release_date = None
        if b"depositionDate" in input_data:
            self.deposition_date = input_data[b"depositionDate"].decode()
        else:
            self.deposition_date = None
        if b"title" in input_data:
            self.title = input_data[b"title"].decode()
        else:
            self.title = None
        if b"mmtfVersion" in input_data:
            self.mmtf_version = input_data[b"mmtfVersion"].decode()
        else:
            self.mmtf_version = None
        if b"mmtfProducer" in input_data:
            self.mmtf_producer = input_data[b"mmtfProducer"].decode()
        else:
            self.mmtf_producer = None
        if b"structureId" in input_data:
            self.structure_id = input_data[b"structureId"].decode()
        else:
            self.structure_id = None
        if b"spaceGroup" in input_data:
            self.space_group = input_data[b"spaceGroup"]
        else:
            self.space_group = None
        if b"bondAtomList" in input_data:
            self.bond_atom_list = np.frombuffer(input_data[b"bondAtomList"][12:],'>i4')
        else:
            self.bond_atom_list = None
        if b"bondOrderList" in input_data:
            self.bond_order_list = np.frombuffer(input_data[b"bondOrderList"][12:],'>i1')
        else:
            self.bond_order_list = None
        if b"secStructList" in input_data:
            self.sec_struct_list = np.frombuffer(input_data[b"secStructList"][12:],'>i1')
        else:
            self.sec_struct_list = []
        if b"insCodeList" in input_data:
            self.ins_code_list = [chr(x) for x in mmtfDecoder.run_length_decoder_numpy(np.frombuffer(input_data[b"insCodeList"][12:],">i4")).astype(np.int16)]
        else:
            self.ins_code_list = []
        if b"atomIdList" in input_data:
            self.atom_id_list = np.cumsum(mmtfDecoder.run_length_decoder_numpy(np.frombuffer(input_data[b'atomIdList'][12:],'>i4')).astype(np.int16))
        else:
            self.atom_id_list = []
        if b"sequenceIndexList" in input_data:
            self.sequence_index_list = np.cumsum(mmtfDecoder.run_length_decoder_numpy(np.frombuffer(input_data[b'sequenceIndexList'][12:],'>i4')).astype(np.int16))
        else:
            self.sequence_index_list = []
        if b"occupancyList" in input_data:
            self.occupancy_list = mmtfDecoder.run_length_decoder_numpy(np.frombuffer(input_data[b"occupancyList"][12:],">i4")) /100
        else:
            self.occupancy_list = []
        if b"entityList" in input_data:
            self.entity_list = mmtfDecoder.decode_entity_list(input_data[b"entityList"])
        else:
            self.entity_list = []
        if b"chainNameList" in input_data:
            self.chain_name_list = [chr(a) for a in input_data[b"chainNameList"][12:][::4]]
        else:
            self.chain_name_list = []
        if b"experimentalMethods" in input_data:
            self.experimental_methods = input_data[b"experimentalMethods"]
        else:
            self.experimental_methods = None

        # Variables gaurenteed in mmtf files
        self.num_bonds = input_data[b"numBonds"]
        self.num_chains = input_data[b"numChains"]
        self.num_models = input_data[b"numModels"]
        self.num_atoms = input_data[b"numAtoms"]
        self.num_groups = input_data[b"numGroups"]
        self.chains_per_model = input_data[b"chainsPerModel"]
        self.groups_per_chain = input_data[b"groupsPerChain"]
        self.group_id_list = np.cumsum(mmtfDecoder.run_length_decoder_numpy(np.frombuffer(input_data[b'groupIdList'][12:],'>i4'))).astype(np.int32)
        self.group_type_list = np.frombuffer(input_data[b'groupTypeList'][12:],'>i4')
        self.chain_id_list = [chr(a) for a in input_data[b"chainIdList"][12:][::4]]
        self.group_list = mmtfDecoder.decode_group_list(input_data[b'groupList'])
        self.x_coord_list = mmtfDecoder.recursive_index_decode(np.frombuffer(input_data[b'xCoordList'][12:],'>i2'),np.frombuffer(input_data[b'xCoordList'][8:12],'>i'))
        self.y_coord_list = mmtfDecoder.recursive_index_decode(np.frombuffer(input_data[b'yCoordList'][12:],'>i2'),np.frombuffer(input_data[b'yCoordList'][8:12],'>i'))
        self.z_coord_list = mmtfDecoder.recursive_index_decode(np.frombuffer(input_data[b'zCoordList'][12:],'>i2'),np.frombuffer(input_data[b'xCoordList'][8:12],'>i'))
        self.alt_loc_list = input_data[b'altLocList'][12:]
        self.alt_loc_set = False


    def pass_data_on(self, data_setters):
        """Write the data from the getters to the setters.
        :param data_setters: a series of functions that can fill a chemical
        data structure
        :type data_setters: DataTransferInterface
        """
        self.set_alt_loc_list()
        data_setters.init_structure(self.num_bonds, len(self.x_coord_list), len(self.group_type_list),
                                    len(self.chain_id_list), len(self.chains_per_model), self.structure_id)
        decoder_utils.add_entity_info(self, data_setters)
        decoder_utils.add_atomic_information(self, data_setters)
        decoder_utils.add_header_info(self, data_setters)
        decoder_utils.add_xtalographic_info(self, data_setters)
        decoder_utils.generate_bio_assembly(self, data_setters)
        decoder_utils.add_inter_group_bonds(self, data_setters)
        data_setters.finalize_structure()


    def set_alt_loc_list(self):
        """
        Set the alternative location list for structure
        """
        #TODO: set inputdata first
        self.alt_loc_list = [chr(x) for x in mmtfDecoder.run_length_decoder_numpy(np.frombuffer(self.alt_loc_list,">i4")).astype(np.int16)]
        self.alt_loc_set = True
        return self
