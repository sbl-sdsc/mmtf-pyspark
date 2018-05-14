#!/user/bin/env python
'''structureToProteinDimers.py:

Maps a structure to its protein dimers

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from mmtf.utils import *
from mmtf.api.mmtf_writer import MMTFEncoder
import itertools
from sympy import Point3D
from mmtfPyspark.utils import DistanceBox
import time
import numpy as np
import math


class StructureToProteinDimers(object):
    '''Maps a protein structure to it's protein dimers

    Attributes
    ----------
    cutoffDistance : float
       cutoff distance for protein dimers [8.0]
    contacts : int
       number of contacts [20]
    useAllAtoms : bool
       flag to use all atoms [False]
    exclusive : bool
       exclusive flag [False]
    '''

    def __init__(self, cutoffDistance=8.0, contacts=20,
                 useAllAtoms=False, exclusive=False):
        self.cutoffDistance = cutoffDistance
        self.contacts = contacts
        self.useAllAtoms = useAllAtoms
        self.exclusive = exclusive

    def __call__(self, t):
        structure = t[1]

        # split structure into a list of chains
        chains = self._split_to_chains(structure)
        chainVectors = self._get_chain_vectors(chains)
        resList = []

        if self.useAllAtoms:
            boxes = self._get_all_atoms_distance_boxes(
                chains, self.cutoffDistance)
        else:
            boxes = self._get_c_beta_atoms_distance_boxes(
                chains, self.cutoffDistance)

        self.exclusiveHashSet = np.empty([0, 3])

        for i in range(len(chains)):

            j = 0

            while j < i:

                if self._check_pair(boxes[i], boxes[j], chains[i], chains[j], self.cutoffDistance, self.contacts):

                    if self.exclusive:

                        newVec = chainVectors[i] - chainVectors[j]
                        if not self._check_list(newVec, self.exclusiveHashSet):

                            resList.append(self._combine_chains(
                                chains[i], chains[j]))

                            self.exclusiveHashSet = np.append(
                                self.exclusiveHashSet, [newVec], axis=0)
                    else:
                        resList.append(self._combine_chains(
                            chains[i], chains[j]))

                j += 1

        return resList

    def _check_list(self, vec, exclusiveList):

        for point in exclusiveList:

            if np.linalg.norm(vec - point) < 0.1 and self._angle(vec, point) < 0.1:
                return True
            vec = vec * -1

            if np.linalg.norm(vec - point) < 0.1 and self._angle(vec, point) < 0.1:
                return True
            vec = vec * -1

        return False

    def _angle(self, a, b):

        arccosInput = np.dot(a, b) / np.linagl.norm(a) / np.linagl.norm(b)
        arccosInput = 1.0 if arccosInput > 1.0 else arccosInput
        arccosInput = -1.0 if arccosInput < -1.0 else arccosInput

        return math.acos(arccosInput)

    def _get_chain_vectors(self, chains):

        chainVectors = []

        for chain in chains:

            chainVectors.append(self._calc_average_vec(chain))

        return chainVectors

    def _calc_average_vec(self, s1):

        totX, totY, totZ = 0, 0, 0

        for i in range(s1.num_atoms):  # TODO double check num atoms

            totX += s1.x_coord_list[i]
            totY += s1.y_coord_list[i]
            totZ += s1.z_coord_list[i]

        return np.array([totX / s1.num_atoms, totY / s1.num_atoms, totZ / s1.num_atoms])

    def _distance(self, s1, s2, index1, index2):

        xCoord = s1.x_coord_list[index1]
        yCoord = s1.y_coord_list[index1]
        zCoord = s1.z_coord_list[index1]

        newPoint1 = Point3D(xCoord, yCoord, zCoord)

        xCoord = s2.x_coord_list[index2]
        yCoord = s2.y_coord_list[index2]
        zCoord = s2.z_coord_list[index2]

        newPoint2 = Point3D(xCoord, yCoord, zCoord)

        return newPoint1.distance(newPoint2)

    def _check_pair(self, box1, box2, s1, s2, cutoffDistance, contacts):

        pointsInBox2 = box1.getIntersection(box2)
        pointsInBox1 = box2.getIntersection(box1)

        hs1, hs2 = set(), set()

        num = 0

        for i in range(len(pointsInBox2)):

            for j in range(len(pointsInBox1)):

                if (i in hs1) or (j in hs2):
                    continue

                if self._distance(s1, s2, pointsInBox2[i], pointsInBox1[j]) < cutoffDistance:

                    num += 1
                    hs1.add(i)
                    hs2.add(j)

                if num > contacts:
                    return True

        return False

    def _get_c_beta_atoms_distance_boxes(self, chains, cutoffDistance):
        '''Get distance boxes for all atoms
        '''
        distanceBoxes = []

        for i in range(len(chains)):

            tmp = chains[i]
            newbox = DistanceBox(cutoffDistance)
            groupIndex = 0
            atomIndex = 0

            for k in range(tmp.groups_per_chain[0]):
                groupType = tmp.group_type_list[groupIndex]

                for m in range(len(tmp.group_list[groupType]["formalChargeList"])):

                    atomName = tmp.group_list[groupType]["atomNameList"][m]

                    if atomName == "CB":

                        xCoord = tmp.x_coord_list[atomIndex]
                        yCoord = tmp.y_coord_list[atomIndex]
                        zCoord = tmp.z_coord_list[atomIndex]

                        # TODO trying numpy instead of Point3D
                        #newPoint = Point3D(xCoord, yCoord, zCoord)
                        newPoint = np.array([xCoord, yCoord, zCoord])

                        newbox.add_point(newPoint, atomIndex)

                    atomIndex += 1

                groupIndex += 1

            distanceBoxes.append(newbox)

        return distanceBoxes

    def _get_all_atoms_distance_boxes(self, chains, cutoffDistance):
        '''Get distance boxes for all atoms
        '''
        distanceBoxes = []

        for i in range(len(chains)):

            tmp = chains[i]
            newbox = DistanceBox(cutoffDistance)

            for j in range(tmp.num_atoms):

                xCoord = tmp.x_coord_list[j]
                yCoord = tmp.y_coord_list[j]
                zCoord = tmp.z_coord_list[j]

                # TODO trying numpy instead of Point3D
                # newPoint = Point3D(xCoord, yCoord, zCoord)
                newPoint = np.array([xCoord, yCoord, zCoord])

                newbox.add_point(newPoint, j)

            distanceBoxes.append(newbox)

        return distanceBoxes

    def _split_to_chains(self, s):
        '''split structure to a list of chains
        '''
        chains = []
        numChains = s.chains_per_model[0]

        chainToEntityIndex = self._get_chain_to_entity_index(s)
        atomsPerChain, bondsPerChain = self._get_num_atoms_and_bonds(s)

        groupCounter = 0
        atomCounter = 0

        for i in range(numChains):
            atomMap = {}
            newChain = MMTFEncoder()
            entityToChainIndex = chainToEntityIndex[i]

            structureId = s.structure_id + '.' +\
                s.chain_name_list[i] + '.' +\
                s.chain_id_list[i] + '.' +\
                str(entityToChainIndex + 1)

            # Set header
            newChain.init_structure(bondsPerChain[i],
                                    atomsPerChain[i],
                                    s.groups_per_chain[i],
                                    1, 1, structureId)
            decoder_utils.add_xtalographic_info(s, newChain)
            decoder_utils.add_header_info(s, newChain)

            # Set model info (only one model: 0)
            newChain.set_model_info(0, 1)

            # Set entity and chain info
            newChain.set_entity_info([0],
                                     s.entity_list[entityToChainIndex]['sequence'],
                                     s.entity_list[entityToChainIndex]['description'],
                                     s.entity_list[entityToChainIndex]['type'])

            newChain.set_chain_info(s.chain_id_list[i],
                                    s.chain_name_list[i],
                                    s.groups_per_chain[i])

            for j in range(s.groups_per_chain[i]):
                groupIndex = s.group_type_list[groupCounter]
                # print(s.group_type_list)

                # Set group info
                newChain.set_group_info(s.group_list[groupIndex]['groupName'],
                                        s.group_id_list[groupCounter],
                                        s.ins_code_list[groupCounter],
                                        s.group_list[groupIndex]['chemCompType'],
                                        len(s.group_list[groupIndex]
                                            ['atomNameList']),
                                        len(s.group_list[groupIndex]
                                            ['bondOrderList']),
                                        s.group_list[groupIndex]['singleLetterCode'],
                                        s.sequence_index_list[groupCounter],
                                        s.sec_struct_list[groupCounter])

                for k in range(len(s.group_list[groupIndex]['atomNameList'])):
                    newChain.set_atom_info(s.group_list[groupIndex]['atomNameList'][k],
                                           s.atom_id_list[atomCounter],
                                           s.alt_loc_list[atomCounter],
                                           s.x_coord_list[atomCounter],
                                           s.y_coord_list[atomCounter],
                                           s.z_coord_list[atomCounter],
                                           s.occupancy_list[atomCounter],
                                           s.b_factor_list[atomCounter],
                                           s.group_list[groupIndex]['elementList'][k],
                                           s.group_list[groupIndex]['formalChargeList'][k])

                    atomCounter += 1

                for l in range(len(s.group_list[groupIndex]['bondOrderList'])):

                    bondIndOne = s.group_list[groupIndex]['bondAtomList'][l * 2]
                    bondIndTwo = s.group_list[groupIndex]['bondAtomList'][l * 2 + 1]
                    bondOrder = s.group_list[groupIndex]['bondOrderList'][l]

                    #newChain.set_group_bond(bondIndOne, bondIndTwo, bondOrder)
                    newChain.current_group.bond_atom_list += [
                        bondIndOne, bondIndTwo]
                    newChain.current_group.bond_order_list.append(bondOrder)

                groupCounter += 1

                # TODO skipping adding inter group bond info for now

            newChain.finalize_structure()

            # TODO double check if just getting from chain 0
            chain_type = [chain['type'] for chain in newChain.entity_list]

            polymer = "polymer" in chain_type

            if polymer:

                match = True

                for j in range(newChain.groups_per_chain[0]):

                    #print(newChain.groups_per_chain, newChain.group_type_list)
                    groupIndex = newChain.group_type_list[j]

                    if match:
                        _type = newChain.group_list[groupIndex]["chemCompType"]

                        match = (
                            _type == "L-PEPTIDE LINKING") or (_type == "PEPTIDE LINKING")

                if match:
                    chains.append(newChain)

        return chains

    def _combine_chains(self, s1, s2):

        if not s1.alt_loc_set:
            s1 = s1.set_alt_loc_list()
        if not s2.alt_loc_set:
            s2 = s2.set_alt_loc_list()

        groupCounter = 0
        atomCounter = 0

        structureId = s1.structure_id + "_append_" + s2.structure_id

        combinedStructure = MMTFEncoder()

        # Set header
        combinedStructure.init_structure(s1.num_bonds + s2.num_bonds,
                                         s1.num_atoms + s2.num_atoms,
                                         s1.num_groups + s2.num_groups,
                                         2, 1, structureId)
        decoder_utils.add_xtalographic_info(s1, combinedStructure)
        decoder_utils.add_header_info(s1, combinedStructure)

        # Set model info (only one model: 0)
        combinedStructure.set_model_info(0, 2)

        chainToEntityIndex = self._get_chain_to_entity_index(s1)[0]

        # Set entity and chain info
        combinedStructure.set_entity_info([0],
                                          s1.entity_list[chainToEntityIndex]['sequence'],
                                          s1.entity_list[chainToEntityIndex]['description'],
                                          s1.entity_list[chainToEntityIndex]['type'])

        combinedStructure.set_chain_info(s1.chain_id_list[0],
                                         s1.chain_name_list[0],
                                         s1.groups_per_chain[0])

        for i in range(s1.groups_per_chain[0]):
            groupIndex = s1.group_type_list[groupCounter]

            # Set group info
            combinedStructure.set_group_info(s1.group_list[groupIndex]['groupName'],
                                             s1.group_id_list[groupCounter],
                                             s1.ins_code_list[groupCounter],
                                             s1.group_list[groupIndex]['chemCompType'],
                                             len(s1.group_list[groupIndex]
                                                 ['atomNameList']),
                                             len(s1.group_list[groupIndex]
                                                 ['bondOrderList']),
                                             s1.group_list[groupIndex]['singleLetterCode'],
                                             s1.sequence_index_list[groupCounter],
                                             s1.sec_struct_list[groupCounter])

            for j in range(len(s1.group_list[groupIndex]['atomNameList'])):
                combinedStructure.set_atom_info(s1.group_list[groupIndex]['atomNameList'][j],
                                                s1.atom_id_list[atomCounter],
                                                s1.alt_loc_list[atomCounter],
                                                s1.x_coord_list[atomCounter],
                                                s1.y_coord_list[atomCounter],
                                                s1.z_coord_list[atomCounter],
                                                s1.occupancy_list[atomCounter],
                                                s1.b_factor_list[atomCounter],
                                                s1.group_list[groupIndex]['elementList'][j],
                                                s1.group_list[groupIndex]['formalChargeList'][j])

                atomCounter += 1

            # TODO not sure if we should add bonds like this
            # TODO bondAtomList == getGroupBondIndices?
            for k in range(len(s1.group_list[groupIndex]["bondOrderList"])):
                bondIndOne = s1.group_list[groupIndex]["bondAtomList"][k * 2]
                bondIndTwo = s1.group_list[groupIndex]["bondAtomList"][k * 2 + 1]
                bondOrder = s1.group_list[groupIndex]["bondOrderList"][k]
                combinedStructure.set_group_bond(
                    bondIndOne, bondIndTwo, bondOrder)

            groupCounter += 1

        # Set entity and chain info for s2
        chainToEntityIndex = self._get_chain_to_entity_index(s2)[0]
        combinedStructure.set_entity_info([1],
                                          s2.entity_list[chainToEntityIndex]['sequence'],
                                          s2.entity_list[chainToEntityIndex]['description'],
                                          s2.entity_list[chainToEntityIndex]['type'])

        combinedStructure.set_chain_info(s2.chain_id_list[0],
                                         s2.chain_name_list[0],
                                         s2.groups_per_chain[0])

        groupCounter = 0
        atomCounter = 0

        for i in range(s2.groups_per_chain[0]):
            groupIndex = s2.group_type_list[groupCounter]

            # Set group info
            combinedStructure.set_group_info(s2.group_list[groupIndex]['groupName'],
                                             s2.group_id_list[groupCounter],
                                             s2.ins_code_list[groupCounter],
                                             s2.group_list[groupIndex]['chemCompType'],
                                             len(s2.group_list[groupIndex]
                                                 ['atomNameList']),
                                             len(s2.group_list[groupIndex]
                                                 ['bondOrderList']),
                                             s2.group_list[groupIndex]['singleLetterCode'],
                                             s2.sequence_index_list[groupCounter],
                                             s2.sec_struct_list[groupCounter])

            for j in range(len(s2.group_list[groupIndex]['atomNameList'])):
                combinedStructure.set_atom_info(s2.group_list[groupIndex]['atomNameList'][j],
                                                s2.atom_id_list[atomCounter],
                                                s2.alt_loc_list[atomCounter],
                                                s2.x_coord_list[atomCounter],
                                                s2.y_coord_list[atomCounter],
                                                s2.z_coord_list[atomCounter],
                                                s2.occupancy_list[atomCounter],
                                                s2.b_factor_list[atomCounter],
                                                s2.group_list[groupIndex]['elementList'][j],
                                                s2.group_list[groupIndex]['formalChargeList'][j])

                atomCounter += 1

            # TODO not sure if we should add bonds like this
            # TODO bondAtomList == getGroupBondIndices?
            for k in range(len(s2.group_list[groupIndex]["bondOrderList"])):
                bondIndOne = s2.group_list[groupIndex]["bondAtomList"][k * 2]
                bondIndTwo = s2.group_list[groupIndex]["bondAtomList"][k * 2 + 1]
                bondOrder = s2.group_list[groupIndex]["bondOrderList"][k]
                combinedStructure.set_group_bond(
                    bondIndOne, bondIndTwo, bondOrder)

            groupCounter += 1

        combinedStructure.finalize_structure()
        return (structureId, combinedStructure)

    def _get_num_atoms_and_bonds(self, structure):
        '''Gets the number of atoms and bonds per chain
        '''
        numChains = structure.chains_per_model[0]
        atomsPerChain = [0] * numChains
        bondsPerChain = [0] * numChains
        groupCounter = 0

        for i in range(numChains):

            for j in range(structure.groups_per_chain[i]):
                groupIndex = structure.group_type_list[groupCounter]
                atomsPerChain[i] = len(
                    structure.group_list[groupIndex]['atomNameList'])
                bondsPerChain[i] = len(
                    structure.group_list[groupIndex]['bondOrderList'])
                groupCounter += 1

        return atomsPerChain, bondsPerChain

    def _get_chain_to_entity_index(self, structure):
        '''Returns an list that maps a chain index to an entity index.

        Parameters
        ----------
        structure: structureDataInterFace
        '''
        entityChainIndex = [0] * structure.num_chains

        for i in range(len(structure.entity_list)):

            for j in structure.entity_list[i]["chainIndexList"]:
                entityChainIndex[j] = i

        return entityChainIndex
