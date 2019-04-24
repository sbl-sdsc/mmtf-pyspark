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

        # Variables that are not in all mmtf files


        if 'resolution' in input_data:
            self.resolution = input_data['resolution']
        else:
            self.resolution = None
        if "rFree" in input_data:
            self.r_free = input_data["rFree"]
        else:
            self.r_free = None
        if "rWork" in input_data:
            self.r_work = input_data["rWork"]
        else:
            self.r_work = None
        if "bioAssemblyList" in input_data:
            self.bio_assembly = input_data["bioAssemblyList"]
        else:
            self.bio_assembly = []
        if "unitCell" in input_data:
            self.unit_cell = input_data["unitCell"]
        else:
            self.unit_cell = None
        if "releaseDate" in input_data:
            self.release_date = input_data["releaseDate"]
        else:
            self.release_date = None
        if "depositionDate" in input_data:
            self.deposition_date = input_data["depositionDate"]
        else:
            self.deposition_date = None
        if "title" in input_data:
            self.title = input_data["title"]
        else:
            self.title = None
        if "mmtfVersion" in input_data:
            self.mmtf_version = input_data["mmtfVersion"]
        else:
            self.mmtf_version = None
        if "mmtfProducer" in input_data:
            self.mmtf_producer = input_data["mmtfProducer"]
        else:
            self.mmtf_producer = None
        if "structureId" in input_data:
            self.structure_id = input_data["structureId"]
        else:
            self.structure_id = None
        if "spaceGroup" in input_data:
            self.space_group = input_data["spaceGroup"]
        else:
            self.space_group = None
        if "bondAtomList" in input_data:
            self.bond_atom_list = np.frombuffer(
                input_data["bondAtomList"][12:], '>i4')
        else:
            self.bond_atom_list = None
        if "bondOrderList" in input_data:
            self.bond_order_list = np.frombuffer(
                input_data["bondOrderList"][12:], '>i1')
        else:
            self.bond_order_list = None

        self.sec_struct_list = mmtfDecoder.decode_type_2(input_data, "secStructList")
        self.atom_id_list = mmtfDecoder.decode_type_8(input_data, "atomIdList", input_data["numAtoms"])
        self.sequence_index_list = mmtfDecoder.decode_type_8(input_data, "sequenceIndexList", input_data["numAtoms"])

        self.b_factor_list = mmtfDecoder.decode_type_10(input_data, "bFactorList")
        self.occupancy_list = mmtfDecoder.decode_type_9(input_data, "occupancyList", input_data["numAtoms"])

        if "experimentalMethods" in input_data:
            self.experimental_methods = input_data["experimentalMethods"]
        else:
            self.experimental_methods = None

        self.ins_code_list = mmtfDecoder.decode_type_6(input_data, "insCodeList", input_data["numGroups"])

        if "entityList" in input_data:
            self.entity_list = input_data["entityList"]
        else:
            self.entity_list = []

        self.chain_name_list = mmtfDecoder.decode_type_5(input_data, "chainNameList")

        # Variables guaranteed in mmtf files
        self.num_bonds = input_data["numBonds"]
        self.num_chains = input_data["numChains"]
        self.num_models = input_data["numModels"]
        self.num_atoms = input_data["numAtoms"]
        self.num_groups = input_data["numGroups"]
        self.chains_per_model = input_data["chainsPerModel"]
        self.groups_per_chain = input_data["groupsPerChain"]
        self.group_id_list = mmtfDecoder.decode_type_8(input_data, "groupIdList", self.num_groups)
        self.group_type_list = mmtfDecoder.decode_type_4(input_data, "groupTypeList")
        self.x_coord_list = mmtfDecoder.decode_type_10(input_data, "xCoordList")
        self.y_coord_list = mmtfDecoder.decode_type_10(input_data, "yCoordList")
        self.z_coord_list = mmtfDecoder.decode_type_10(input_data, "zCoordList")
        self.group_list = input_data['groupList']
        self.chain_id_list = mmtfDecoder.decode_type_5(input_data, "chainIdList")
        self.alt_loc_list = mmtfDecoder.decode_type_6(input_data, "altLocList", self.num_atoms)

    def pass_data_on(self, data_setters):
        """Write the data from the getters to the setters.

        Parameters
        ----------
        data_setters : DataTransferInterface
            a series of functions that can fill a chemical
        """
        data_setters.init_structure(self.num_bonds, len(self.x_coord_list), len(self.group_type_list),
                                    len(self.chain_id_list), len(self.chains_per_model), self.structure_id)
        decoder_utils.add_entity_info(self, data_setters)
        decoder_utils.add_atomic_information(self, data_setters)
        decoder_utils.add_header_info(self, data_setters)
        decoder_utils.add_xtalographic_info(self, data_setters)
        decoder_utils.generate_bio_assembly(self, data_setters)
        decoder_utils.add_inter_group_bonds(self, data_setters)
        data_setters.finalize_structure()

