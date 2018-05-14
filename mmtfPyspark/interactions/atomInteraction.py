#!/user/bin/env python
'''atomInteraction.py:

AtomInteraction contains interaction information of a central atom with it's
interacting neighbors (coordination sphere). Once this data structure is filled
with interaction centers, this class calculates various geometric properties
such as distance, angles, and order parameters for the interacting atoms.
Finally, it provides methods for creating row-rise representations of the data
in Spark Datasets.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from mmtfPyspark.interactions import CoordinateGeometry, InteractionCenter
from pyspark.sql import Row
from pyspark.sql.types import *
import numpy as np


class AtomInteraction(object):

    q3 = None
    q4 = None
    q5 = None
    q6 = None
    distances = None
    angles = None

    def __init__(self):

        self.structure_id = None
        self.center = None
        self.neighbors = []

    def set_structure_id(self, structureId):
        '''Sets the structure identifier

        Parameters
        ----------
        structureId : str
           the structure identifier

        '''

        self.structureId = structureId

    def get_structure_id(self):
        '''Returns the structure identifier

        Returns
        -------
        str
           structure identifier

        '''

        return self.structureId

    def set_center(self, center):
        '''Sets the central atom information of a coordination sphere.

        Parameters
        ----------
        center : InteractionCenter
           central atom information

        '''
        self.center = center

    def get_center(self):
        '''Returns information about the central atom of a coordination sphere

        Returns
        -------
        list
           centeral atom information
        '''

        return self.center

    def add_neighbor(self, neighbor):
        '''Adds a neighbor interaction center.

        Parameters
        ----------
        neighbor : InteractionCenter
           an interation with the central atom

        '''

        self.neighbors.append(neighbor)

    def get_interactions(self):
        '''Returns information about the interacting neighbor atoms.

        Returns
        -------
        list
           interaction centers

        '''
        return self.neighbors

    def get_num_interactions(self):
        '''Returns the number of neighboring atoms that interact with the central atom

        Returns
        -------
        int
           number of neighboring atoms that interact with the central atom

        '''
        return len(self.neighbors)

    def calc_coordination_geometry(self, maxInteraction):
        '''Calculates geometric properties of the coordination sphere.
        The geometric properties include orientational order parameters that
        describe the arrangement of the atoms in the coordination sphere,
        distances and angles of the neighcor atoms with the cnter atom.

        Parameters
        ----------
        maxInteraction : int
           maximum number of interaction

        '''
        neighborPoints = [n.get_coordinates() for n in self.neighbors
                          if n.get_coordinates() is not None]

        geom = CoordinateGeometry(
            self.center.get_coordinates(), neighborPoints)

        # calculate distances to the central atom
        #self.distances = np.empty(maxInteraction)
        self.distances = [0.0] * maxInteraction
        for i, dist in enumerate(geom.get_distance()):
            self.distances[i] = dist

        # calculate angles among all interacting atoms with the central atom
        numInteraction = int(maxInteraction * (maxInteraction - 1) / 2)
        ang = geom.get_angles()
        self.angles = [np.NaN] * numInteraction
        self.angles[:len(ang[:numInteraction])] = ang[:numInteraction]

        # TODO: points or neighbor points
        if len(neighborPoints) > 2:
            self.q3 = geom.q3()
        if len(neighborPoints) > 3:
            self.q4 = geom.q4()
        if len(neighborPoints) > 4:
            self.q5 = geom.q5()
        if len(neighborPoints) > 5:
            self.q6 = geom.q6()

    def get_multiple_interactions_as_row(self, maxInteractions):
        '''Returns interactions and geometric information in a single row

        Returns
        -------
        int
           row of itneractions and geometric information

        '''
        while self.get_num_interactions() < maxInteractions:
            self.neighbors.append(InteractionCenter())

        self.length = InteractionCenter.get_length()

        self.calc_coordination_geometry(maxInteractions)

        data = [self.structureId, self._get_number_of_polymer_chains(),
                self.q3, self.q4, self.q5, self.q6]

        # Copy data for query atom
        data += self.center.get_as_object()

        # Copy data for interaction atoms
        for i, neighbor in enumerate(self.neighbors):
            data += neighbor.get_as_object()
            data.append(self.distances[i])

        data += self.angles

        return Row(data)

    def get_pair_interactions_as_rows(self):
        '''Return rows of pairwise interactions with the central atom

        Returns
        -------
        list
           rows of pairwise interactions with the central atom

        '''
        rows = []
        length = InteractionCenter.get_length()
        self.calc_coordination_geometry()

        for i, neighbor in enumerate(self.neighbors):
            index = 0

        return rows

    def get_schema(self, maxInteractions):
        '''Returns the schema for a row of atom interaction inforamtion. The schema
        is used to create a Dataset<Row> from the row information.

        Parameters
        ----------
        maxInteraction : int
           maximum number of interactions

        Returns
        -------
        pyspark.sql.types.StructType
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
        sf += InteractionCenter.get_struct_fields(0)

        # Copy schema info for interacting atoms and their distances
        for i in range(maxInteractions):
            sf += InteractionCenter.get_struct_fields(i + 1)
            sf.append(StructField(f"distance{i+1}", FloatType(), True))

        # Add schema for angles
        for i in range(maxInteractions - 1):
            for j in range(i + 1, maxInteractions):
                sf.append(StructField(f"angle{i+1}-{j+1}", FloatType(), True))

        return StructType(sf)

    def get_pair_interaction_schema(self):
        '''Returns the schema for a row of pairwise atom interactions.
        The schema is used to create a Dataset<Row> from the row information

        Returns
        -------
        pyspark.sql.types.StructType
           schema for dataset

        '''

        sf = []
        sf.append(StructField("pdbId", StringType(), False))

        # copy schema info for query atom
        sf += InteractionCenter.get_struct_fields(0)

        # copy schema infor for interacting atoms and their distnce
        sf += InteractionCenter.get_struct_fields(1)
        sf.append(StructField("distance1", FloatType(), True))

        return StructType(sf)

    def _get_number_of_polymer_chains(self):
        '''Returns the number of unique polymer chains in the coordination sphere.

        Returns
        -------
            number of unique polymer chains in the coordination sphere.
        '''

        return len({center.get_chain_name for center in self.neighbors
                    if center.atomName is not None and center.get_sequence_position() >= 0})

    def _get_num_columns(self, maxInteractions):
        '''Returns the number of columns in a Row

        The number of columns:
            structureId + polymerChains + q3 + q4 + q5 + q6: 6
            query + interaction centers: (maxInteractions + 1) * length
            distances: maxInteractions
            angles: maxInteractions * (maxInteractions - 1) / 2

        Parameters
        ----------
        maxInteractions : int
           maximum number of interactions

        Returns
        -------
        int
           number of columns in a row
        '''

        numAngles = maxInteractions * (maxInteractions - 1) / 2
        return int(6 + (maxInteractions + 1) * self.length + maxInteractions * numAngles)

    def _set(self, data, value, i):
        '''Sets the ith index of data to value and increase i by one
        
        Parameters
        ----------
        data : list
        value : int
           value to increase
        i : int
           ith index of data to value

        '''
        data[i] = value
        i += 1
        return data, i
