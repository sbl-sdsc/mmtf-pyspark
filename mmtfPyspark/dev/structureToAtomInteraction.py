#!/user/bin/env python
'''
structureToAtomInteraction.py

Finds interactions that match the criteria specified by the InteractionFilter


Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "dev"
'''

from mmtfPyspark.dev import InteractionFilter, ColumnarStructureX

class StructureToAtomInteraction(object):
    '''
    Attributes:
        bfilter (Class): Specifies the conditions for calculating interactions
        pairwise (bool): If True, results as one row per pair interactions.
                         If False, the interactions of one atom with all other
                         atoms are returned as a single row.
    '''

    def __init__(self, bfilter, pairwise = False):

        self.filter = bfilter.getValue()
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
            # TODO Make get_interactions function (need atomInteractions)


    def _get_distance_box(self, arrays):
        '''Add atom indices on grid for rapid indexing of atom neighbors on a
        grid based on a cutoff distance
        '''

        # Get required data
        x = arrays.get_x_coords()
        y = arrays.get_y_coords()
        z = arrays.get_z_coords()
        elements = arrays.get_elements()
        groupsNames = arrays.get_group_elements()

        # TODO: Point3D slow, see if nessassary
        box = [[x[i],y[i],z[i]] for i in range(arrays.get_num_atoms()) \
               if self.filter.is_target_group(groupNames[i]) \
               and self.filter.is_target_element(elements[i])]

        return box


    def _get_query_atom_indices(self, arrays):
        '''Returns a list of indices to query atoms in the structure
        '''

        # Get required data
        groupNames = arrays.get_group_names()
        elements = arrays.get_elements()
        groupStartIndices = arrays.get_group_to_atom_indices()
        occupancies = arrays.get_occupancies()

        # Find atoms that match the query criteria and exlcued atoms with
        # partial occupancy
        indices = []
        for i in range(arrays.get_num_groups()):

            start = groupStartIndices[i]
            end = groupStartIndices[i+1]

            if self.filter.is_query_group(groupNames[start]):
                indices += [j for j in range(start, end) \
                            if self.filter.is_query_element(elements[j]) \
                            and normalizedbFactors[j] < self.filter.get_normalized_b_factor_cutoff() \
                            and occupancies[j] >= 1.0]

        return indices
