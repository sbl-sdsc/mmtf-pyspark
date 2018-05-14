#!/user/bin/env python
'''chemicalStructureQuery.py

This filter returns entries that contain groups with specified chemical structures (SMILES string).
This chemical structure query supports for query: exact, similar, substructure, and superstructure.
For details see references.

References
----------
- `Chemical Structure Search <http://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/chemSmiles.html>`_

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.webfilters import AdvancedQuery


class ChemicalStructureQuery(object):

    EXACT = "Exact"
    SIMILAR = "Similar"
    SUBSTRUCTURE = "Substructure"
    SUPERSTRUCTURE = "Superstructure"

    def __init__(self, smiles, queryType="Substructure", percentSimilarity=0.0):
        '''Constructor to setup filter that matches any entry with at least one
        chemical component that matches the specified SMILES string using the
        specified query type.

        For details see:
        `Chemical Structure Search <http://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/chemSmiles.html>`_

        Parameters
        ----------
        smiles : str
           SMILES string representing chemical structure
        queryType : str
           One of the 4 supported types
        percentSimilarity : float
           percent similarity for similarity search. This parameter is ignored
           for all other query types [default: 0.0]
        '''

        if not (queryType == self.EXACT
                or queryType == self.SIMILAR
                or queryType == self.SUBSTRUCTURE
                or queryType == self.SUPERSTRUCTURE):

            raise Exception("Invalid search type: %s" % queryType)

        query = "<orgPdbQuery>" + \
            "<queryType>org.pdb.query.simple.ChemSmilesQuery</queryType>" + \
            "<smiles>" + smiles + "</smiles>" + \
            "<searchType>" + queryType + "</searchType>" + \
            "<similarity>" + str(percentSimilarity) + "</similarity>" + \
            "<polymericType>Any</polymericType>" + \
            "</orgPdbQuery>"

        self.filter = AdvancedQuery(query)

    def __call__(self, t):
        return self.filter(t)
