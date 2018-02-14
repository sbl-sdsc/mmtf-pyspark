#!/user/bin/env python
'''
atomInteraction.py:

AtomInteraction contains interaction information of a central atom with it's
interacting neighbors (coordination sphere). Once this data structure is filled
with interaction centers, this class calculates various geometric properties
such as distance, angles, and order parameters for the interacting atoms.
Finally, it provides methods for creating row-rise representations of the data
in Spark Datasets.

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "debug"
'''

from mmtfPyspark.dev import CoordinateGeometry
from pyspark.sql import Row
from pyspark.types import *
import numpy as np

class AtomInteraction(object):

    def __init__(self):

        self.structure_id = None
        self.center = None
        self.neighbors = []


    def set_structure_id(self, structureId):
        '''Sets the structure identifier

        Attributes:
            structureId (str): the structure identifier
        '''

        self.structureId = structureId


    def get_structure_id(self):
        '''Returns the structure identifier

        Returns:
            structure identifier
        '''

        return self.structureId


    def set_center(self, center):
        '''Sets the central atom information of a coordination sphere.

        Attributes:
            center (InteractionCenter): central atom information
        '''

        self.center = center


    def get_center(self):
        '''Returns information about the central atom of a coordination sphere

        Returns:
            centeral atom information
        '''

        return self.center


    def add_neighbor(self, neighbor):
        '''Adds a neighbor interaction center.

        Attributes:
            neighbor (InteractionCenter): an interation with the central atom
        '''

        self.neighbors.append(neighbor)


    def get_interactions(self):
        '''
        Returns information about the interacting neighbor atoms.

        Returns:
            interaction centers
        '''

        return self.neighbors


    def get_num_interactions(self):
        '''
        Returns the number of neighboring atoms that interact with the central
        atom

        Returns:
            number of neighboring atoms that interact with the central atom
        '''

        return len(self.neighbors)


    def calc_coordination_geometry(self, maxInteraction):
        '''
        Calculates geometric properties of the coordination sphere.
        The geometric properties include orientational order parameters that
        describe the arrangement of the atoms in the coordination sphere,
        distances and angles of the neighcor atoms with the cnter atom.

        Attributes:
            maxInteraction (int): maximum number of interaction
        '''

        neighborPoints = [n.get_coordinates for n in self.neighbors \
                          if n.get_coordinates is not None]

        geom = CoordinateGeometry(self.center.get_coordinates, neighborPoints)

        # calculate distances to the central atom
        self.distances = np.empty(maxInteractions)
        for i, dist in enumerate(geom.get_distance()):
            self.distances[i] = dist

        # calculate angles among all interacting atoms with the central atom
        numInteraction = maxInteractions * (maxInteractions - 1) / 2
        self.angles = np.empty(numInteraction)
        for i, ang in enumerate(geom.get_angles()):
            self.angles[i] = ang

        # TODO: points or neighbor points
        if len(neighborPoints) > 2:
            self.q3 = geom.q3()
        if len(neighborPoints) > 3:
            self.q4 = geom.q4()
        if len(neighborPoints) > 4:
            self.q5 = geom.q5()
        if len(neighborPoints) > 5:
            self.q6 = geom.q6()


    def get_multiple_interactions_as_row(self,maxInteractions):
        '''Returns interactions and geometric information in a single row

        Returns:
            row of itneractions and geometric information
        '''

        while self.get_num_interactions < maxInteractions:
            self.neighbors.append(InteractionCenter())

        self.length = InteractionCenter().get_length()

        index = 0
        self.calc_coordination_geometry()
        data = np.empty(self._get_num_columns(maxInteractions))
        data, index = self._set(data, self.structureId, index)
        data, index = self._set(data, self._get_number_of_polymer_chains(), index)
        data, index = self._set(data, self.q3, index)
        data, index = self._set(data, self.q4, index)
        data, index = self._set(data, self.q5, index)
        data, index = self._set(data, self.q6, index)

        # Copy data for query atom
        data[index: index + self.length] = self.center.get_as_object()
        index += self.length

        # Copy data for interacting atoms
        for i,neighbor in enumerate(self.neighbors):
            data[index : index + self.length] = neighbor.get_as_object()
            index += self.length
            data, index = self._set(data, self.distance[i], index)

        # Copy angles
        data[index: index + len(self.angles)] = self.angles

        return Row(data)


    def get_pair_interactions_as_rows(self):
        '''Return rows of pairwise interactions with the central atom

        Returns:
            rows of pairwise interactions with the central atom
        '''

        rows = []
        length = InteractionCenter().get_length()
        self.calc_coordination_geometry()

        for i, neighbor in enumerate(self.neighbors):
            index = 0
            data = np.empty(2*length + 2)

            data, index = self._set(data, self.structureId, index)
            data[index: index + length] = self.center.get_as_object()
            index += length

            data[index: index + length] = neighbor.get_as_object()
            index += length

            data[index] = self.distance[i]

            rows.append(Row(data))

        return rows


    def get_schema(self, maxInteractions):
        '''
        Returns the schema for a row of atom interaction inforamtion. The schema
        is used to create a Dataset<Row> from the row information.

        Returns:
            schema for dataset
        '''

        sf = []
        sf.append(StructField("pdbId", StringType(), False))
        sf.append(StructField("polyChains", IntegerType(), False))
        sf.append(StructField("q3", FloatType(), True))
        sf.append(StructField("q4", FloatType(), True))
        sf.append(StructField("q5", FloatType(), True))
        sf.append(StructField("q6", FloatType(), True))

        # Copy schema for query atom
        sf += InteractionCenter().get_struct_fields(0)

        # Copy schema info for interacting atoms and their distances
        for i in range(maxInteractions):
            sf += InteractionCenter().get_struct_fields(i)
            sf.append(StructField(f"distance{i+1}", FloatType(), True))

        # Add schema for angles
        for i,j in zip(range(maxInteractions - 1), range(1, maxInteractions)):
            sf.append(StructField(f"angle{i+1}-{j+1}", FloatType(), True))

        return StructType(sf)


    def get_pair_interaction_schema(self):
        '''
        Returns the schema for a row of pairwise atom interactions.
        The schema is used to create a Dataset<Row> from the row information

        Return:
            schema for dataset
        '''

        sf = []
        sf.append(StructField("pdbId", StringType(), False))

        # copy schema info for query atom
        sf += InteractionCenter().get_struct_fields(0)

        # copy schema infor for interacting atoms and their distnce
        sf += InteractionCenter().get_struct_fields(1)
        sf.append(StructField("distance1", FloatType(), True))

        return StructType(sf)


    def _get_number_of_polymer_chains(self):
        '''
        Returns the number of unique polymer chains in the coordination sphere.

        Returns:
            number of unique polymer chains in the coordination sphere.
        '''

        return len({center.get_chain_name for center in self.neighbors \
                   if center is not None and center.get_sequence_position() >=0})


    def _get_num_columns(self, maxInteractions):
        '''Returns the number of columns in a Row

        The number of columns:
            structureId + polymerChains + q3 + q4 + q5 + q6: 6
            query + interaction centers: (maxInteractions + 1) * length
            distances: maxInteractions
            angles: maxInteractions * (maxInteractions - 1) / 2

        Attributes:
            maxInteractions (int): maximum number of interactions

        Returns:
            number of columns in a row
        '''

        numAngles = maxInteractions * (maxInteractions - 1) / 2
        return 6 + (maxInteractions + 1) * self.length + maxInteractions * numAngles


    def _set(self, data, value, i):
        '''
        Sets the ith index of data to value and increase i by one
        '''

        data[index] = value
        i += 1
        return data, i
