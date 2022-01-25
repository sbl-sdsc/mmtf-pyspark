#!/user/bin/env python
'''wildType.py

This filter returns entries that contain wild type protein chains.
polymer chain(s) made of L-amino acids. If the "exclusive" flag is set to true
in the constructor, all polymer chains must be L-proteins. For a multi-model structure,
this filter only checks the first model.
'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.webfilters import AdvancedQuery


class WildTypeQuery(object):

    SEQUENCE_COVERAGE_100 = 100
    SEQUENCE_COVERAGE_95 = 95
    SEQUENCE_COVERAGE_90 = 90
    SEQUENCE_COVERAGE_85 = 85
    SEQUENCE_COVERAGE_80 = 80
    SEQUENCE_COVERAGE_75 = 75
    SEQUENCE_COVERAGE_70 = 70
    SEQUENCE_COVERAGE_65 = 65
    SEQUENCE_COVERAGE_60 = 60

    def __init__(self, includeExpressionTags, percentSequenceCoverage=None):
        '''Default constructor maches an entry that contains at least one L-protein chain.
        As an example, an L-protein/DNA complex passes this filter

        Parameters
        ----------
        includeExpressionTags : bool
           flag to include expression tags
        percentSequenceCoverage : int
           percentage of sequence converage [NONE]
        '''

        query = "<orgPdbQuery><queryType>org.pdb.query.simple.WildTypeProteinQuery</queryType>"

        if includeExpressionTags:
            query += "<includeExprTag>Y</includeExprTag>"
        else:
            query += "<includeExprTag>N</includeExprTag>"

        if not percentSequenceCoverage == None:
            query += "<percentSeqAlignment>%i</percentSeqAlignment>"%percentSequenceCoverage

        query += "</orgPdbQuery>"

        self.filter = AdvancedQuery(query)

    def __call__(self, t):
        return self.filter(t)
