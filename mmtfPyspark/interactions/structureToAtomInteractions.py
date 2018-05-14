#!/user/bin/env python
'''structureToAtomInteraction.py

Finds interactions that match the criteria specified by the InteractionFilter

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from mmtfPyspark.interactions import InteractionFilter, AtomInteraction, InteractionCenter
from mmtfPyspark.utils import ColumnarStructureX
from mmtfPyspark.utils import DistanceBox
import numpy as np


class StructureToAtomInteractions(object):
    '''Class that finds structure to atom intteractions.

    Attributes
    ----------
    bfilter : obj
       Specifies the conditions for calculating interactions
    pairwise : bool
       If True, results as one row per pair interactions.
       If False, the interactions of one atom with all other 
       atoms are returned as a single row.
    '''

    def __init__(self, bfilter, pairwise=False):

        self.filter = bfilter.value
        self.pairwise = pairwise

    def __call__(self, t):

        interactions = []
        structureId = t[0]

        # convert structure to an array-based format for efficient processing
        arrays = ColumnarStructureX(t[1], True)

        # create a list of query atoms for which interactions should be calculated
        queryAtomIndices = self._get_query_atom_indices(arrays)

        if len(queryAtomIndices) == 0:
            return interactions

        # Add atom (indices) on grid for rapid indexing of atom neighbors on a
        # grid based on a cutoff distance
        box = self._get_distance_box(arrays)

        for queryAtomIndex in queryAtomIndices:
            # find interactions of query atom specified by atom index
            interaction = self._get_interactions(arrays, queryAtomIndex, box)
            interaction.set_structure_id(structureId)

            # only add interations that are within the given limits of interations
            if interaction.get_num_interactions() >= self.filter.get_min_interactions() \
                    and interaction.get_num_interactions() <= self.filter.get_max_interactions():

                # return interactions as either pairs or all interaction of
                # one atom as a row
                if self.pairwise:
                    interactions += interaction.get_pair_interactions_as_rows()
                else:
                    multiInteract = interaction.get_multiple_interactions_as_row(
                        self.filter.get_max_interactions())
                    interactions += multiInteract

        return interactions

    def _get_interactions(self, arrays, queryAtomIndex, box):
        '''Get the interacting neighbors of an atom in a structure

        Parameters
        ----------
        arrays : columnarStructure
           structure in columnarStructure format
        queryAtomIndex : int
           the index of the querying atom
        box : distanceBox
           the distance box of the query atom

        Returns
        -------
        AtomInteraction
           an AtomInteraction class with interacting neighbors
        '''
        interaction = AtomInteraction()

        # get the x,y,z coordinates of the structure
        x = arrays.get_x_coords()
        y = arrays.get_y_coords()
        z = arrays.get_z_coords()

        # get the query atom coordinates
        qx = x[queryAtomIndex]
        qy = y[queryAtomIndex]
        qz = z[queryAtomIndex]

        # get required information of the columnarStructure
        atomToGroupIndices = arrays.get_atom_to_group_indices()
        occupancies = arrays.get_occupancies()
        normalizedbFactors = arrays.get_normalized_b_factors()
        groupNames = arrays.get_group_names()

        # record query atom info
        queryCenter = InteractionCenter(arrays, queryAtomIndex)
        interaction.set_center(queryCenter)

        # Retrieve atom indices of atoms that lay within grid cubes that are
        # within cutoff distance of the query atom
        cutoffDistanceSq = self.filter.get_distance_cutoff() ** 2

        # Retrieve atom indices of atoms that lay within grid cubes
        # that are within cutoff distance of the query atom
        neighborIndices = box.get_neighbors(np.array([qx, qy, qz]))
        # TEST: flattern neighborIndices
        if type(neighborIndices[0]) == list:
            neighborIndices = [
                n for neighbors in neighborIndices for n in neighbors]

        # determine and record interactions with neighbor atoms
        for neighborIndex in neighborIndices:

            # exclude self interactions with a group
            if atomToGroupIndices[neighborIndex] == atomToGroupIndices[queryAtomIndex]:
                continue

            # check if interaction is within distance cutoff
            dx = qx - x[neighborIndex]
            dy = qy - y[neighborIndex]
            dz = qz - z[neighborIndex]
            distSq = dx * dx + dy * dy + dz * dz

            if distSq <= cutoffDistanceSq:
                # Exclude interactions with undesired groups and
                # atoms with partial occupancy (< 1.0)
                if self.filter.is_prohibited_target_group(groupNames[neighborIndex]) \
                        or self.filter.get_normalized_b_factor_cutoff() < normalizedbFactors[neighborIndex] \
                        or occupancies[neighborIndex] < float(1.0):

                    # return an empty atom interaction
                    return AtomInteraction()

                # add interacting atom info
                neighbor = InteractionCenter(arrays, neighborIndex)
                interaction.add_neighbor(neighbor)

                # terminate early if the number of interactions exceeds limit
                if interaction.get_num_interactions() > self.filter.get_max_interactions():
                    return interaction

        return interaction

    def _get_distance_box(self, arrays):
        '''Add atom indices on grid for rapid indexing of atom neighbors on a
        grid based on a cutoff distance

        Parameters
        ----------
        arrays : columnarStructure
           structure in columnarStructure format
        '''

        # Get required data
        x = arrays.get_x_coords()
        y = arrays.get_y_coords()
        z = arrays.get_z_coords()
        elements = arrays.get_elements()
        groupNames = arrays.get_group_names()

        box = DistanceBox(self.filter.get_distance_cutoff())
        for i in range(arrays.get_num_atoms()):
            if self.filter.is_target_group(groupNames[i]) \
                    and self.filter.is_target_element(elements[i]):

                newPoint = np.array([x[i], y[i], z[i]])
                box.add_point(newPoint, i)

        return box

    def _get_query_atom_indices(self, arrays):
        '''Returns a list of indices to query atoms in the structure

        Parameters
        ----------
        arrays : columnarStructure
           structure in columnarStructure format
        '''

        # Get required data
        groupNames = arrays.get_group_names()
        elements = arrays.get_elements()
        groupStartIndices = arrays.get_group_to_atom_indices()
        occupancies = arrays.get_occupancies()
        normalizedbFactors = arrays.get_normalized_b_factors()

        # Find atoms that match the query criteria and exlcued atoms with
        # partial occupancy
        indices = []
        for i in range(arrays.get_num_groups()):

            start = groupStartIndices[i]
            end = groupStartIndices[i + 1]

            if self.filter.is_query_group(groupNames[start]):
                indices += [j for j in range(start, end)
                            if self.filter.is_query_element(elements[j])
                            and normalizedbFactors[j] < self.filter.get_normalized_b_factor_cutoff()
                            and occupancies[j] >= 1.0]

        return indices
