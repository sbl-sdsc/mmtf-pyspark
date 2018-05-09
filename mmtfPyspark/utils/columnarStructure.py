#!/user/bin/env python
'''columnarStructure.py

Provides efficient access to structure information in the form of atom-based arrays.
Data are lazily initialized as needs.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import numpy as np


class ColumnarStructure(object):
    '''Column based data structure to efficiently access structure information

    Attributes
    ----------
    structure : mmtfStructure
       mmtf structure
    firstModelOnly : bool
       use only the first model in a structure if True
    '''

    def __init__(self, structure, firstModelOnly=True):

        # Set class variables
        self.numAtoms = 0
        self.numChains = 0
        self.numGroups = 0
        self.numModels = 0

        self.atomNames = None
        self.atomToChainIndices = None
        self.atomToGroupIndices = None
        self.chainIds = None
        self.chainNames = None
        self.chemCompType = None
        self.elements = None
        self.entityTypes = None
        self.entityChainIndex = None
        self.entityIndices = None
        self.groupNames = None
        self.groupNumbers = None
        self.groupToAtomIndices = None
        self.polymer = None
        self.sequencePositions = None
        self.structure = structure
        self.groupToAtomIndices = None
        self.chainToAtomIndices = None
        self.chainToGroupIndices = None


        if firstModelOnly:
            self.numModels = 1
        else:
            self.numModels = structure.num_models

    def get_group_to_atom_indices(self):
        if self.groupToAtomIndices is None:
            self.get_indices()
        return self.groupToAtomIndices

    def get_chain_to_atom_indices(self):
        if self.chainToAtomIndices is None:
            self.get_indices()
        return self.chainToAtomIndices

    def get_chain_to_group_indices(self):
        if self.chainToGroupIndices is None:
            self.get_indices
        return self.chainToGroupIndices

    def get_num_atoms(self):
        self.get_indices()
        return self.numAtoms

    def get_num_groups(self):
        self.get_indices()
        return self.numGroups

    def get_num_chains(self):
        self.get_indices()
        return self.numChains

    def get_num_models(self):
        self.get_indices()
        return self.numModels

    def get_x_coords(self):
        return self.structure.x_coord_list

    def get_y_coords(self):
        return self.structure.y_coord_list

    def get_z_coords(self):
        return self.structure.z_coord_list

    def get_occupancies(self):
        return self.structure.occupancy_list

    def get_b_factors(self):
        return self.structure.b_factor_list

    def get_alt_loc_list(self):
        if not self.structure.alt_loc_set:
            self.structure = self.structure.set_alt_loc_list()
        return self.structure.alt_loc_list

    def get_group_types(self):
        return self.structure.group_type_list

    def get_atom_to_group_indices(self):

        if self.atomToGroupIndices is None:
            self.atomToGroupIndices = np.empty(
                self.get_num_atoms(), dtype='>i4')

            for i in range(self.get_num_groups()):
                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]
                self.atomToGroupIndices[start:end] = i

        return self.atomToGroupIndices

    def get_chem_comp_types(self):

        if self.chemCompType is None:
            # Max chemCompType has 17 characters
            self.chemCompType = np.empty(
                self.get_num_atoms(), dtype=np.object_)
            self.groupTypeIndices = self.get_group_types()

            for i in range(self.get_num_groups()):
                index = self.groupTypeIndices[i]
                value = self.structure.group_list[index]['chemCompType']
                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]
                self.chemCompType[start:end] = value

        return self.chemCompType

    def get_elements(self):

        if self.elements is None:
            self.elements = np.empty(self.get_num_atoms(), dtype=np.object_)
            self.groupTypeIndices = self.get_group_types()

            for i in range(self.get_num_groups()):
                index = self.groupTypeIndices[i]
                elementNames = self.structure.group_list[index]['elementList']
                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]
                self.elements[start:end] = elementNames

        return self.elements

    def get_atom_names(self):

        if self.atomNames is None:
            # in case of 2 cjacter group + 3 digits
            self.atomNames = np.empty(self.get_num_atoms(), dtype=np.object_)
            self.groupTypeIndices = self.get_group_types()

            for i in range(self.get_num_groups()):
                index = self.groupTypeIndices[i]
                atomNames = self.structure.group_list[index]['atomNameList']
                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]
                self.atomNames[start:end] = atomNames

        return self.atomNames

    def get_entity_types(self):

        if self.entityTypes is None:
            self.entityTypes = np.empty(self.get_num_atoms(), dtype=np.object_)

            # Instantiate required data
            self.is_polymer()
            self.get_chem_comp_types()
            self.get_elements()
            self.get_group_names()

            for i in range(self.get_num_groups()):
                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]
                poly = self.polymer[start]
                ccType = self.chemCompType[start]

                if poly:
                    if "PEPTIDE" in ccType:
                        eType = "PRO"
                    elif "DNA" in ccType:
                        eType = "DNA"
                    elif "RNA" in ccType:
                        eType = "RNA"
                    elif "SACCHARIDE" in ccType:
                        eType = "PSR"
                    else:
                        eType = "UNK"
                elif (self.groupNames[start] == "HOH" or self.groupNames[start] == "DOD"):
                    eType = "WAT"
                elif "SACCHARIDE" in ccType:
                    eType = "SAC"
                else:
                    # If group contains at least one carbon atom, it is
                    # considered organic, but see expection below
                    organic = False
                    for j in range(start, end):
                        if self.elements[j] == "C":
                            organic = True
                            break

                    # Handle exceptions: carbon dioxide, carbon monoxide,
                    # cyanide ion are considered inorganic
                    if self.groupNames[start] == "CO2" or \
                       self.groupNames[start] == "CMO" or \
                       self.groupNames[start] == "CYN":
                        organic = False

                    if organic:
                        eType = "LGO"
                    else:
                        eType = "LGI"

                self.entityTypes[start:end] = eType

        return self.entityTypes

    def get_group_names(self):

        if self.groupNames is None:
            self.groupNames = np.empty(self.get_num_atoms(), dtype=np.object_)
            self.groupTypeIndices = self.structure.group_type_list

            for i in range(self.get_num_groups()):
                index = self.groupTypeIndices[i]
                value = self.structure.group_list[index]['groupName']
                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]
                self.groupNames[start:end] = value

        return self.groupNames

    def get_group_numbers(self):

        if self.groupNumbers is None:
            # In case of 4 digit group id + 2 ins code
            self.groupNumbers = np.empty(
                self.get_num_atoms(), dtype=np.object_)

            for i in range(self.get_num_groups()):
                value = str(self.structure.group_id_list[i])
                insCode = self.structure.ins_code_list[i]

                if insCode != "\x00":
                    value += insCode

                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]
                self.groupNumbers[start:end] = value

        return self.groupNumbers

    def get_chain_ids(self):

        if self.chainIds is None:
            self.chainIds = np.empty(self.get_num_atoms(), dtype=np.object_)

            for i in range(self.get_num_chains()):
                start = self.chainToAtomIndices[i]
                end = self.chainToAtomIndices[i + 1]
                self.chainIds[start:end] = self.structure.chain_id_list[i]

        return self.chainIds

    def get_chain_names(self):

        if self.chainNames is None:
            self.chainNames = np.empty(self.get_num_atoms(), dtype=np.object_)

            for i in range(self.get_num_chains()):
                start = self.chainToAtomIndices[i]
                end = self.chainToAtomIndices[i + 1]
                self.chainNames[start:end] = self.structure.chain_name_list[i]

        return self.chainNames

    def is_polymer(self):

        if self.polymer is None:
            # TODO: Numpy bool type is different than python bools, might need to fix
            self.polymer = np.empty(self.get_num_atoms(), dtype=bool)
            self.get_chain_to_entity_index()

            for i in range(self.get_num_chains()):
                index = self.entityChainIndex[i]
                start = self.chainToAtomIndices[i]
                end = self.chainToAtomIndices[i + 1]
                poly = self.structure.entity_list[index]['type'] == 'polymer'

                self.polymer[start:end] = poly

        return self.polymer

    def get_entity_indices(self):

        if self.entityIndices is None:
            self.entityIndices = np.empty(
                self.get_num_atoms(), dtype=np.object_)
            self.get_chain_to_entity_index()

            for i in range(self.get_num_chains()):
                start = self.chainToAtomIndices[i]
                end = self.chainToAtomIndices[i + 1]

                self.entityIndices[start:end] = self.entityChainIndex[i]

        return self.entityIndices

    def get_atom_to_chain_indices(self):

        if self.atomToChainIndices is None:
            self.atomToChainIndices = np.empty(
                self.get_num_atoms(), dtype='>i4')

            for i in range(self.get_num_chains()):
                start = self.chainToAtomIndices[i]
                end = self.chainToAtomIndices[i + 1]

                self.atomToChainIndices[start:end] = i

        return self.atomToChainIndices

    def get_sequence_positions(self):

        if self.sequencePositions is None:

            self.sequencePositions = np.empty(
                self.get_num_atoms(), dtype='>i4')
            groupSequenceIndices = self.structure.sequence_index_list

            for i in range(self.get_num_groups()):
                value = groupSequenceIndices[i]
                start = self.groupToAtomIndices[i]
                end = self.groupToAtomIndices[i + 1]

                self.sequencePositions[start:end] = value

        return self.sequencePositions

    def get_chain_to_entity_index(self):
        '''Returns an array that maps a chain index to an entity index

        Returns
        -------
        :obj:`array <numpy.ndarray>`
           index that maps chain index to an entity index
        '''

        if self.entityChainIndex is None:

            self.entityChainIndex = np.empty(
                self.structure.num_chains, dtype='>i4')

            for i, entity in enumerate(self.structure.entity_list):

                chainIndexList = entity['chainIndexList']
                self.entityChainIndex[chainIndexList] = i

        return self.entityChainIndex

    def get_indices(self):

        if self.groupToAtomIndices is None:

            self.groupToAtomIndices = np.empty(
                self.structure.num_groups + 1, dtype='>i4')
            self.chainToAtomIndices = np.empty(
                self.structure.num_chains + 1, dtype='>i4')
            self.chainToGroupIndices = np.empty(
                self.structure.num_chains + 1, dtype='>i4')

            chainCount, groupCount, atomCount = 0, 0, 0

            # Loop over all models
            for m in range(self.numModels):

                # Loop over all chains
                for i in range(self.structure.chains_per_model[m]):

                    self.chainToAtomIndices[chainCount] = atomCount
                    self.chainToGroupIndices[chainCount] = groupCount

                    # Loop over all groups in chain
                    for j in range(self.structure.groups_per_chain[i]):

                        groupType = self.structure.group_type_list[groupCount]
                        self.groupToAtomIndices[groupCount] = atomCount
                        atomsInGroup = len(
                            self.structure.group_list[groupType]['formalChargeList'])

                        groupCount += 1
                        atomCount += atomsInGroup

                    chainCount += 1

            self.groupToAtomIndices[groupCount] = atomCount
            self.chainToAtomIndices[chainCount] = atomCount
            self.chainToGroupIndices[chainCount] = groupCount

            self.numAtoms = atomCount
            self.numGroups = groupCount
            self.numChains = chainCount

            if self.numAtoms < self.structure.num_atoms:
                self.groupToAtomIndices = self.groupToAtomIndices[:groupCount + 1]
                self.chainToAtomIndices = self.chainToAtomIndices[:chainCount + 1]
                self.chainToGroupIndices = self.chainToGroupIndices[:chainCount + 1]
