#!/user/bin/env python
'''polymerInteractionFIngerprint.py

Finds interactions between polymer chains and maps them onto polymer sequences.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from mmtfPyspark.interactions import InteractionFilter, AtomInteraction, InteractionCenter
from mmtfPyspark.utils import ColumnarStructure
from mmtfPyspark.utils import DistanceBox
from pyspark.sql import Row
import numpy as np


class PolymerInteractionFingerprint(object):

    def __init__(self, interactionFilter):
        self.filter = interactionFilter

    def __call__(self, t):

        structureId = t[0]
        structure = t[1]
        return self.get_interactions(structureId, structure)

    def get_interactions(self, structureId, structure):
        rows = []

        cutoffDistanceSquared = self.filter.get_distance_cutoff() ** 2
        arrays = ColumnarStructure(structure, True)

        chainNames = arrays.get_chain_names()
        groupNames = arrays.get_group_names()
        groupNumbers = arrays.get_group_numbers()
        atomNames = arrays.get_atom_names()
        entityIndices = arrays.get_entity_indices()
        elements = arrays.get_elements()
        polymer = arrays.is_polymer()

        sequenceMapIndices = arrays.get_sequence_positions()
        x = arrays.get_x_coords()
        y = arrays.get_y_coords()
        z = arrays.get_z_coords()

        # create a distance box for quick lookup interactions of polymer atoms
        # of the specified elements
        boxes = {}
        for i in range(arrays.get_num_atoms()):

            if polymer[i] \
                and (self.filter.is_target_group(groupNames[i]) or self.filter.is_query_group(groupNames[i])) \
                and (self.filter.is_target_atom_name(atomNames[i]) or self.filter.is_query_atom_name(atomNames[i])) \
                and (self.filter.is_target_element(elements[i]) or self.filter_is_query_element_name(elements[i])) \
                and not self.filter.is_prohibited_target_group(groupNames[i]):

                if chainNames[i] not in boxes:
                    box = DistanceBox(self.filter.get_distance_cutoff())
                    boxes[chainNames[i]] = box

                newPoint = np.array([x[i],y[i],z[i]])
                boxes[chainNames[i]].add_point(newPoint,i)

        chainBoxes = [(k,v) for k,v in boxes.items()]

        # loop over all pairwise polymer chain interactions
        for i in range(len(chainBoxes) - 1):
            chainI = chainBoxes[i][0]
            boxI = chainBoxes[i][1]

            for j in range(i+1, len(chainBoxes)):
                chainJ = chainBoxes[j][0]
                boxJ = chainBoxes[j][1]

                intersectionI = boxI.getIntersection(boxJ)
                intersectionJ = boxJ.getIntersection(boxI)

                # maps to store sequence indices mapped to group numbers
                indicesI = {}
                indicesJ = {}

                entityIndexI = -1
                entityIndexJ = -1

                # loop over pairs of atom interactions and check if
                # they satisfy the interaction filter criteria

                for n in intersectionI:

                    for m in intersectionJ:

                        dx = x[n] - x[m]
                        dy = y[n] - y[m]
                        dz = z[n] - z[m]
                        dSq = dx * dx + dy * dy + dz * dz

                        if dSq <= cutoffDistanceSquared:
                            if self.filter.is_target_group(groupNames[n]) \
                                and self.filter.is_target_atom_name(atomNames[n]) \
                                and self.filter.is_target_element(elements[n]) \
                                and self.filter.is_query_group(groupNames[m]) \
                                and self.filter.is_query_atom_name(atomNames[m]) \
                                and self.filter.is_query_element(elements[m]):

                                entityIndexI = entityIndices[n]
                                indicesI[sequenceMapIndices[n]] = groupNumbers[n]

                            if self.filter.is_target_group(groupNames[m]) \
                                and self.filter.is_target_atom_name(atomNames[m]) \
                                and self.filter.is_target_element(elements[m]) \
                                and self.filter.is_query_group(groupNames[n]) \
                                and self.filter.is_query_atom_name(atomNames[n]) \
                                and self.filter.is_query_element(elements[n]):

                                entityIndexJ = entityIndices[m]
                                indicesJ[sequenceMapIndices[m]] = groupNumbers[m]

            if len(indicesI) >= self.filter.get_min_interactions():
                sequenceIndiciesI = sorted([int(i) for i in indicesI.keys()])
                groupNumbersI = sorted(list(indicesI.values()))

                rows.append(Row(structureId + '.' + chainI, chainJ, chainI, \
                                groupNumbersI, sequenceIndiciesI, \
                                structure.entity_list[entityIndexI]['sequence']))

            if len(indicesJ) >= self.filter.get_min_interactions():
                sequenceIndiciesJ = sorted([int(i) for i in indicesJ.keys()])
                groupNumbersJ = sorted(list(indicesJ.values()))

                rows.append(Row(structureId + '.' + chainJ, chainI, chainJ, \
                                groupNumbersJ, sequenceIndiciesJ, \
                                structure.entity_list[entityIndexJ]['sequence']))

        return rows
