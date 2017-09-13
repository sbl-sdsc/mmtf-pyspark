import numpy
import numpy as np
import time
import struct

class mmtf_structure(object):
    def run_length_decoder_numpy(self,in_array):
        '''

        switch=False
        out_array=[]
        in_array = in_array.tolist()
        for item in in_array:
            if switch==False:
                this_item = item
                switch=True
            else:
                switch=False
                out_array.extend([this_item]*int(item))
        return np.asarray(out_array, dtype=np.int32)

        '''
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
        '''

        n = len(int_array) - (np.count_nonzero(int_array == 32767) + np.count_nonzero(int_array == -32768)) + 1
        out_arr = np.full(n, np.nan)
        count = 0
        decoded_val = 0
        for item in int_array.tolist():
            if item==max or item==min:
                decoded_val += item
            else:
                decoded_val += item
                out_arr[count] = decoded_val
                count += 1
                #out_arr.append(decoded_val)
                decoded_val = 0


        return out_arr


        out_arr = []
        decoded_val = 0
        for item in int_array.tolist():
            if item==max or item==min:
                decoded_val += item
            else:
                decoded_val += item
                out_arr.append(decoded_val)
                decoded_val = 0
        return numpy.asarray(out_arr,dtype=numpy.int32)

        out_arr = np.full(n, np.nan)
        buf, val, i = 0, 0, 0
        for diff in int_array.tolist():
            if diff == max or diff == min:
                buf += diff
            else:
                val += (diff + buf)
                out_arr[i] = val
                buf = 0
                i += 1
        #out_arr = np.full(n,np.nan)
        out_arr = []
        decoded_val = 0
        #count = 0
        #while count_np < len(int_array):
        #for i in range(n):
        for item in int_array.tolist():
            if item==max or item==min:
            #if int_array[count_np] == max or int_array[count_np] == min:
                decoded_val +=  item
                #count += 1
            else:
                #decoded_val += item
                #decoded_val = int_array[count_np]
                #out_arr[count] = item + decoded_val
                out_arr.append(item + decoded_val)
                #count_np += 1
                #count += 1
                decoded_val = 0

        return np.cumsum(np.asarray(out_arr, dtype = np.int32))/1000
        #return np.cumsum(out_arr)/1000
        '''




    def run_length_decoder(self,l):
        x = [0]
        for v,l in zip(l[::2],l[1::2]):
            x += [v]*l
        return x[1:]

    def set_coord_list(self):
        self.x_coord_list = np.cumsum(self.recursive_index_decode(self.x_coord_list))/1000
        self.y_coord_list = np.cumsum(self.recursive_index_decode(self.y_coord_list))/1000
        self.z_coord_list = np.cumsum(self.recursive_index_decode(self.z_coord_list))/1000
        return self

    def __init__(self, input_data):

        #self.x_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'xCoordList'][12:],'>i2'), np.frombuffer(input_data[b'xCoordList'][4:8],'>i')[0])
        #self.y_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'yCoordList'][12:],'>i2'), np.frombuffer(input_data[b'yCoordList'][4:8],'>i')[0])
        #self.z_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'zCoordList'][12:],'>i2'), np.frombuffer(input_data[b'zCoordList'][4:8],'>i')[0])


        self.x_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'xCoordList'][12:],'>i2'))
        self.y_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'yCoordList'][12:],'>i2'))
        self.z_coord_list = self.recursive_index_decode(np.frombuffer(input_data[b'zCoordList'][12:],'>i2'))

        #self.x_coord_list = np.cumsum(self.recursive_index_decode(np.frombuffer(input_data[b'xCoordList'][12:],'>i2')))/1000
        #self.y_coord_list = np.cumsum(self.recursive_index_decode(np.frombuffer(input_data[b'yCoordList'][12:],'>i2')))/1000
        #self.z_coord_list = np.cumsum(self.recursive_index_decode(np.frombuffer(input_data[b'zCoordList'][12:],'>i2')))/1000
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
        if b"experimentalMethods" in input_data:
            self.experimental_methods = [x.decode() for x in input_data[b"experimentalMethods"]]
        else:
            self.experimental_methods = None
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
        if b"chainNameList" in input_data:
            self.chain_name_list = [chr(a) for a in input_data[b"chainNameList"][12:][::4]]
        else:
            self.chain_name_list = []


        self.num_bonds = input_data[b"numBonds"]
        self.num_chains = input_data[b"numChains"]
        self.num_models = input_data[b"numModels"]
        self.num_atoms = input_data[b"numAtoms"]
        self.num_groups = input_data[b"numGroups"]
        self.chains_per_model = input_data[b"chainsPerModel"]
        self.groups_per_chain = input_data[b"groupsPerChain"]


        self.chain_id_list = [chr(a) for a in input_data[b"chainIdList"][12:][::4]]
        self.group_type_list = np.frombuffer(input_data[b'groupTypeList'][12:],'>i4') # double check /1000


        # TODO dictionary in bytes
        if b"entityList" in input_data:
            self.entity_list = input_data[b"entityList"]
        else:
            self.entity_list = []
        self.group_list = input_data[b"groupList"]


        if b"occupancyList" in input_data:
            self.occupancy_list = self.run_length_decoder_numpy(np.frombuffer(input_data[b"occupancyList"][12:],">i4")) /100
        else:
            self.occupancy_list = []

        if b"altLocList" in input_data:
            self.alt_loc_list = [chr(int(a)) for a in self.run_length_decoder_numpy(np.frombuffer(input_data[b'altLocList'][12:],">i4"))]
            #self.alt_loc_list = self.run_length_decoder_numpy(np.frombuffer(input_data[b'altLocList'][12:],">i4")).astype(np.int16)

        else:
            self.alt_loc_list = []

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
