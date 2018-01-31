#!/user/bin/env python
'''
columnarStructureX.py

Inheritance class of ColumnarStructure

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "dev"
'''
import numpy as np
from mmtfPyspark.io import ColumnarStructure


class ColumnarStructureX(ColumnarStructure):
    '''Inheritance of class ColumnarStructure with additional functions
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
            # Filter out DOD and HOH
            stats = self.bFactors[(self.entityTypes != "HOH") & (self.entityTypes != "DOD")]
            # Define normalize function
            normalize = lambda x: (x - stats.mean()) / stats.std()
            self.normalizedbFactors = normalize(self.bFactors)

        return self.normalizedbFactors


    def get_clamped_normalized_b_factors(self):
        '''Returns a normalized B-factors that are clamped to the [-1,1] interval
        using the method of Liu et at. B-factors are normalized and scaled the
        90% Confidenceinterval of the B-factors to [-1,1]. Any value outside of
        the 90% confidence interval is set to either -1 or 1, whichever is closer.

        Reference:
            Liu et al. BMC Bioinformatics 2014, 15(Suppl 16):S3,
                Use B-factor related features for accurate classification between
                protein binding interfaces and crystal packing contacts
                <"https://doi.org/10.1186/1471-2105-15-S16-S3">
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
