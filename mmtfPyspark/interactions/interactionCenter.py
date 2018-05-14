#!/user/bin/env python
'''interactionCenter.py:

InteractionCenter stores information about an atom involved in a molecular
interaction.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from pyspark.sql.types import *
import numpy as np


class InteractionCenter(object):
    '''Class that stores information about an atom involved in a molecular
    interaction.

    Attributes
    ----------
    structure : mmtfStructure
       structure to be used as interaction center [None]
    atomIndex : int
       the index of the atom at center [None]
    '''

    def __init__(self, structure=None, atomIndex=None):

        if structure is not None and atomIndex is not None:

            self.set_atom_name(structure.get_atom_names()[atomIndex])
            self.set_element(structure.get_elements()[atomIndex])
            self.set_group_name(structure.get_group_names()[atomIndex])
            self.set_group_number(structure.get_group_numbers()[atomIndex])
            self.set_type(structure.get_entity_types()[atomIndex])
            self.set_chain_name(structure.get_chain_names()[atomIndex])
            self.set_sequence_position(
                structure.get_sequence_positions()[atomIndex])
            self.set_coordinates(np.array([structure.get_x_coords()[atomIndex],
                                           structure.get_y_coords()[atomIndex],
                                           structure.get_z_coords()[atomIndex]]))
            self.set_normalized_b_factors(
                structure.get_normalized_b_factors()[atomIndex])

        else:
            self.coordinates = None
            self.atomName = None
            self.element = None
            self.groupName = None
            self.groupNumber = None
            self.type = None
            self.chainName = None
            self.normalizedbFactor = None

    def get_length():
        '''Returns the number of data items in an interaction center.
        Note, not all data are currently included

        Returns
        -------
            the number of data items in an interaction center
        '''

        LENGTH = 7

        return LENGTH

    def get_atom_name(self):
        '''Gets the atom name

        Returns
        -------
            name of the atom
        '''

        return self.atomName

    def set_atom_name(self, atomName):
        '''Sets the atom name

        Parameters
        ----------
            atomName (str): name of the atom
        '''

        self.atomName = atomName

    def get_element(self):
        '''Gets the case-sensitive element symbol

        Returns
        -------
            element symbol
        '''

        return self.element

    def set_element(self, element):
        '''Sets the case-sensitive element symbol

        Parameters
        ----------
            element (str): element symbol
        '''

        self.element = element

    def get_group_name(self):
        '''Gets the names of the group. This name is the chemical component id
        of this group

        Returns
        -------
            name of group
        '''

        return self.groupName

    def set_group_name(self, groupName):
        '''Sets the names of the group. This name is the chemical component id
        of this group

        Parameters
        ----------
            groupName (str): name of group
        '''

        self.groupName = groupName

    def get_group_number(self):
        '''Gets the group number for the interaction center. A group number
        consists of the residue number (e.g. 101) plus an optional insertion code
        (e.g. A): 101A.

        Returns
        -------
            group number
        '''

        return self.groupNumber

    def set_group_number(self, groupNumber):
        '''Sets the group number for the interaction center. A group number
        consists of the residue number (e.g. 101) plus an optional insertion code
        (e.g. A): 101A.

        Parameters
        ----------
            groupNumber (str): group number
        '''

        self.groupNumber = groupNumber

    def get_type(self):
        '''Gets the type of the group.

        Returns
        -------
            type of group
        '''

        return self.type

    def set_type(self, gtype):
        '''Sets the type of the group.

        Parameters
        ----------
            gtype (str): type of group
        '''

        self.type = gtype

    def get_chain_name(self):
        '''Gets the chainName. This corresponds to the "chain Id" in PDB files.

        Returns
        -------
            the name of the chain
        '''

        return self.chainName

    def set_chain_name(self, chainName):
        '''Sets the chainName. This corresponds to the "chain Id" in PDB files.

        Parameters
        ----------
            chainName (str): the name of the chian
        '''

        self.chainName = chainName

    def get_sequence_position(self):
        '''Gets an index into the one-letter polymer sequence. this index is
        zero-based. If the interaction center is not a polymer atom, this index
        is -1

        Returns
        -------
            index into polymer sequence
        '''

        return self.sequencePosition

    def set_sequence_position(self, sequencePosition):
        '''Sets an index into the one-letter polymer sequence. this index is
        zero-based. If the interaction center is not a polymer atom, this index
        is -1

        Parameters
        ----------
            sequencePosition (int): index into polymer sequence
        '''

        self.sequencePosition = sequencePosition

    def get_coordinates(self):
        '''Gets the position of the interaction center.

        Returns
        -------
            the position of the interaction center
        '''

        return self.coordinates

    def set_coordinates(self, coordinates):
        '''Sets the position of the interaction center.

        Parameters
        ----------
            coordinates (list): the position of the interaction center
        '''

        self.coordinates = coordinates

    def get_normalized_b_factors(self):
        '''Gets the normalized b-factor

        Returns
        -------
            the normalized b-factor
        '''

        return self.normalizedbFactor

    def set_normalized_b_factors(self, normalizedbFactor):
        '''Sets the normalized b-factor

        Parameters
        ----------
            normalizedbFactor (float) the normalized b-factor
        '''

        self.normalizedbFactor = normalizedbFactor

    def get_as_object(self):
        '''Returns a list of objects representing this interaction center. Note,
        not all data are currently included. This method is ucsed to create
        Spark datasets.

        Returns
        -------
            list of objects representing this interaction center
        '''

        return [self.atomName, self.element, self.groupName, self.groupNumber,
                self.type, self.chainName, self.normalizedbFactor]

    def get_struct_fields(index):
        '''Returns a schema to create Spark Datasets. This schema must match the
        order in which the data are return by the getAsObject() method.

        Parameters
        ----------
            index (int): an integer to label an interaction center

        Returns
        -------
            schema to represent an interaction center in spark dataset.
        '''

        sf = []
        nullable = True
        index = str(index)

        sf.append(StructField("atom" + index, StringType(), nullable))
        sf.append(StructField("element" + index, StringType(), nullable))
        sf.append(StructField("group" + index, StringType(), nullable))
        sf.append(StructField("groupNum" + index, StringType(), nullable))
        sf.append(StructField("type" + index, StringType(), nullable))
        sf.append(StructField("chain" + index, StringType(), nullable))
        sf.append(StructField("nbFactor" + index, FloatType(), nullable))

        return sf
