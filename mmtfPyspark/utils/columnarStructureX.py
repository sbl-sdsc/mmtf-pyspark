#!/user/bin/env python
'''columnarStructureX.py

Inheritance class of ColumnarStructure

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import numpy as np
import sys
from mmtfPyspark.utils import ColumnarStructure
from sympy import Point3D


class ColumnarStructureX(ColumnarStructure):
    '''Inheritance of class ColumnarStructure with additional functions

    Attributes
    ----------
    structure : mmtfStructure)
       mmtf structure
    firstModelOnly : bool
       flag to use only the first model of the structure
    '''

    def __init__(self, structure, firstModelOnly = True):

        ColumnarStructure.__init__(self, structure, firstModelOnly)
        self.normalizedbFactors = None
        self.clampedNormalizedbFactor = None


    def get_normalized_b_factors(self):
        '''Returns z-scores for B-factors (normalized B-factors).

        Critical z-score values: Confidence level Tail Area z critical
        90% 0.05 +- 1.645
        95% 0.025 +- 1.96
        99% 0.005 +- 2.576
        '''

        if self.normalizedbFactors is None:

            self.get_entity_types()
            self.bFactors = self.get_b_factors()
            self.entityTypes = self.get_entity_types()
            # Filter out DOD and HOH
            stats = np.array([self.bFactors[i] for i in range(self.get_num_atoms())\
                              if self.entityTypes[i] is not 'WAT'])
            # Define normalize function
            normalize = lambda x: (x - stats.mean()) / stats.std()
            if stats.std() != 0:
                self.normalizedbFactors = [float(n) for n in normalize(self.bFactors)]
            else:
                self.normalizedbFactors = [sys.float_info.max] * len(self.bFactors)

        return self.normalizedbFactors


    def get_clamped_normalized_b_factors(self):
        '''Returns a normalized B-factors that are clamped to the [-1,1] interval
        using the method of Liu et at. B-factors are normalized and scaled the
        90% Confidenceinterval of the B-factors to [-1,1]. Any value outside of
        the 90% confidence interval is set to either -1 or 1, whichever is closer.

        References
        ----------
        - Liu et al. BMC Bioinformatics 2014, 15(Suppl 16):S3,
          Use B-factor related features for accurate classification between
          protein binding interfaces and crystal packing contacts 
          https://doi.org/10.1186/1471-2105-15-S16-S3
        '''

        if self.clampedNormalizedbFactor is None:

            self.get_normalized_b_factors()
            self.clampedNormalizedbFactor = self.normalizedbFactors.copy()

            # Normalize and scale the 90% confidence interval of the B factor to [-1,1]
            self.clampedNormalizedbFactor = self.clampedNormalizedbFactor / 1.645

            # Set any value outside the 90% interval to either -1 or 1
            self.clampedNormalizedbFactor[self.clampedNormalizedbFactor < -1.0] = -1.0
            self.clampedNormalizedbFactor[self.clampedNormalizedbFactor > 1.0] = 1.0

        return self.clampedNormalizedbFactor


    def get_calpha_coordinates(self):
        '''Get the coordinates for Calpha atoms
        '''

        self.get_calpha_atom_indices()

        x = self.get_x_coords()
        y = self.get_y_coords()
        z = self.get_z_coords()

        # TODO: Point3D extremely slow, only use if nessassary
        #calpha_coords_list = [Point3D(x[i], y[i], z[i]) for i in self.caIndices]
        calpha_coords_list = [np.array([x[i], y[i], z[i]]) for i in self.caIndices]
        self.calpha_coords = np.array(calpha_coords_list)

        return self.calpha_coords


    def get_calpha_atom_indices(self):
        '''Get the indices of Calpha atoms
        '''

        self.get_entity_types()
        self.get_atom_names()

        caIndices_list = [i for i in range(self.get_num_atoms()) \
                          if (self.atomNames[i] == "CA" \
                          and self.entityTypes[i] == "PRO")]

        self.caIndices = np.array(caIndices_list)

        return self.caIndices
