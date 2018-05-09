#!/user/bin/env python
'''ligandInteractionFIngerprint.py

Finds interactions between ligands and polymer chains and maps them onto polymer sequences.

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


class LigandInteractionFingerprint(object):

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
        box = DistanceBox(self.filter.get_distance_cutoff())
        for i in range(arrays.get_num_atoms()):

            if polymer[i] \
                and self.filter.is_target_group(groupNames[i]) \
                and self.filter.is_target_atom_name(atomNames[i]) \
                and self.filter.is_target_element(elements[i]) \
                and not self.filter.is_prohibited_target_group(groupNames[i]):

                newPoint = np.array([x[i],y[i],z[i]])
                box.add_point(newPoint, i)

        groupToAtomIndices = arrays.get_group_to_atom_indices()

        for g in range(arrays.get_num_groups()):

            # position of first and last atom +1 in group
            start = groupToAtomIndices[g]
            end = groupToAtomIndices[g+1]

            # skip polymer groups
            if polymer[start]:
                continue

            # the specified filter conditions (some groups may be excluded,
            # e.g. water)
            if self.filter.is_query_group(groupNames[start]):

                print(groupNames[start])
                # create list of atoms that interact within the cutoff distance
                neighbors = []
                for a in range(start,end):

                    if self.filter.is_query_atom_name(atomNames[a]) \
                        and self.filter.is_query_element(elements[a]):

                        p = np.array([x[a], y[a], z[a]])

                        # loop up neighbors that are within a cubic
                        for j in box.get_neighbors(p):
                            dx = x[j] - x[a]
                            dy = y[j] - y[a]
                            dz = z[j] - z[a]
                            dSq = dx * dx + dy * dy + dz * dz

                            if dSq <= cutoffDistanceSquared:
                                neighbors.append(j)

                if len(neighbors) == 0:
                    continue

                interactions2 = {}
                for neighbor in neighbors:

                    if chainNames[neighbor] not in interactions2:
                        interactions2[chainNames[neighbor]] = []

                    # keep track of which group is interacting
                    seqPos = sequenceMapIndices[neighbor]

                    # non-polymer groups have a negative index and are exlcuded here
                    if seqPos > 0:
                        l = [seqPos, groupNumbers[neighbor], entityIndices[neighbor]]
                        interactions2[chainNames[neighbor]].append(l)

                for key, val in interactions2.items():

                    sequenceIndices = set()
                    residueNames = set()
                    sequence = None

                    for v in val:
                        sequenceIndices.add(int(v[0]))
                        residueNames.add(int(v[1]))
                        if sequence is None:
                            sequence = structure.entity_list[v[2]]['sequence']

                    if len(sequenceIndices) > 0:
                        rows.append(Row(structureId + "." + key, groupNames[start], \
                                        groupNumbers[start], chainNames[start], \
                                        key, sorted(list(residueNames)), \
                                        sorted(list(sequenceIndices)), sequence,\
                                        len(interactions2)))
        return rows
