import numpy
import numpy as np
import time
import struct

class mmtf_structure(object):
    def run_length_decoder_numpy(self,in_array):
        """
        """
        lengths = np.array(in_array[1::2])
        values = np.array(in_array[0::2])
        starts = np.insert(np.array([0]),1,np.cumsum(lengths))[:-1]
        ends = starts + lengths
        n = ends[-1]
        x = np.full(n, np.nan)
        for l,h,v in zip(starts, ends, values):
            x[l:h] = v
        return x


    def recursive_index_decode(self, int_array, max=32767, min=-32768):
        """Unpack an array of integers using recursive indexing.
        :param int_array: the input array of integers
        :param max: the maximum integer size
        :param min: the minimum integer size
        :return the array of integers after recursive index decoding"""

        out_arr = np.cumsum(int_array)/1000
        return out_arr[(int_array != max) & (int_array != min)]


    def decode_entity_list(self, input_data):
        """Convert byte strings to strings in the entity list.
        :param input_data the list of entities
        :return the decoded entity list"""

        out_data = []
        for entry in input_data:
            out_data.append(self.convert_entity(entry))
        return out_data


    def decode_group_list(self, input_data):
        """Convert byte strings to strings in the group map.
        :param input_data the list of groups
        :return the decoded group list"""

        out_data = []
        for entry in input_data:
            out_data.append(self.convert_group(entry))
        return out_data


    def convert_group(self, input_group):
        """Convert an individual group from byte strings to regula strings.
        :param input_group the input group
        :return the decoded group"""
        output_group = {}
        for key in input_group:
            if key in [b'elementList', b'atomNameList']:
                output_group[key.decode('ascii')] = [x.decode('ascii')
                                                     for x in input_group[key]]
            elif key in [b'chemCompType', b'groupName', b'singleLetterCode']:
                output_group[key.decode('ascii')] = input_group[key].decode('ascii')
            else:
                output_group[key.decode('ascii')] = input_group[key]
        return output_group


    def convert_entity(self, input_entity):
        """Convert an individual entity from byte strings to regular strings
        :param input_entity the entity to decode
        :return the decoded entity"""
        output_entity = {}
        for key in input_entity:
            if key in [b'description', b'type', b'sequence']:
                output_entity[key.decode(
                    'ascii')] = input_entity[key].decode('ascii')
            else:
                output_entity[key.decode('ascii')] = input_entity[key]
        return output_entity

        self.x_coord_list = np.cumsum(self.recursive_index_decode(self.x_coord_list))/1000
        self.y_coord_list = np.cumsum(self.recursive_index_decode(self.y_coord_list))/1000
        self.z_coord_list = np.cumsum(self.recursive_index_decode(self.z_coord_list))/1000
        return self

    def __init__(self, input_data):

        self.x_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'xCoordList'][12:],'>i2'))
        self.y_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'yCoordList'][12:],'>i2'))
        self.z_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'zCoordList'][12:],'>i2'))
        if b"bFactorList" in input_data:
            self.b_factor_list = self.recursive_index_decode(np.frombuffer(input_data[b'xCoordList'][12:],'>i2'))
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
            self.title = input_data[b"title"]
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
            self.space_group = input_data[b"spaceGroup"].decode()
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


        self.num_bonds = input_data[b"numBonds"]
        self.num_chains = input_data[b"numChains"]
        self.num_models = input_data[b"numModels"]
        self.num_atoms = input_data[b"numAtoms"]
        self.num_groups = input_data[b"numGroups"]
        self.chains_per_model = input_data[b"chainsPerModel"]
        self.groups_per_chain = input_data[b"groupsPerChain"]



        self.group_type_list = np.frombuffer(input_data[b'groupTypeList'][12:],'>i4') # double check /1000


        # TODO dictionary in bytes
        if b"entityList" in input_data:
            self.entity_list = self.decode_entity_list(input_data[b"entityList"])
        else:
            self.entity_list = []
        self.group_list = self.decode_group_list(input_data[b"groupList"])


        if b"occupancyList" in input_data:
            self.occupancy_list = self.run_length_decoder_numpy(np.frombuffer(input_data[b"occupancyList"][12:],">i4")) /100
        else:
            self.occupancy_list = []



        if b"insCodeList" in input_data:
            self.ins_code_list = self.run_length_decoder_numpy(np.frombuffer(input_data[b"insCodeList"][12:],">i4"))
        else:
            self.ins_code_list = []

        self.group_id_list = np.cumsum(self.run_length_decoder_numpy(np.frombuffer(input_data[b'groupIdList'][12:],'>i4')).astype(np.int16))

        if b"atomIdList" in input_data:
            self.atom_id_list =np.cumsum(self.run_length_decoder_numpy(np.frombuffer(input_data[b'atomIdList'][12:],'>i4')).astype(np.int16))
        else:
            self.atom_id_list = []

        if b"sequenceIndexList" in input_data:
            self.sequence_index_list = np.cumsum(self.run_length_decoder_numpy(np.frombuffer(input_data[b'sequenceIndexList'][12:],'>i4')).astype(np.int16))
        else:
            self.sequence_index_list = []




        # TODO dictionary in bytes
        if b"entityList" in input_data:
            self.entity_list = self.decode_entity_list(input_data[b"entityList"])
        else:
            self.entity_list = []
        self.group_list = self.decode_group_list(input_data[b"groupList"])




        self.chain_id_list = [chr(a) for a in input_data[b"chainIdList"][12:][::4]]

        if b"chainNameList" in input_data:
            self.chain_name_list = [chr(a) for a in input_data[b"chainNameList"][12:][::4]]
        else:
            self.chain_name_list = []
        if b"experimentalMethods" in input_data:
            self.experimental_methods = [x.decode() for x in input_data[b"experimentalMethods"]]
        else:
            self.experimental_methods = None
        '''
        if b"altLocList" in input_data:
            self.alt_loc_list = [chr(a) for a in self.run_length_decoder_numpy(np.frombuffer(input_data[b'altLocList'][12:],">i4")).astype(np.int16)]
            #self.alt_loc_list = self.run_length_decoder_numpy(np.frombuffer(input_data[b'altLocList'][12:],">i4")).astype(np.int16)
        else:
            self.alt_loc_list = []
        '''
