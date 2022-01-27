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

from mmtfPyspark.webservices.advancedQueryService import post_query


class ChemicalStructureQuery(object):

    SIMILAR_STEREOSPECIFIC = 'graph-relaxed-stereo'
    SIMILAR_STEREOISOMERS = 'graph-relaxed'
    SIMILAR = 'fingerprint-similarity'
    SUBSTRUCTURE_STEREOSPECIFIC = 'sub-struct-graph-relaxed-stereo'
    SUBSTRUCTURE_STEREOISOMERS = 'sub-struct-graph-relaxed-stereo'
    EXACT_MATCH = 'graph-exact'

    def __init__(self, smiles, match_type=SUBSTRUCTURE_STEREOSPECIFIC, percentSimilarity=0.0):
        '''Constructor to setup filter that matches any entry with at least one
        chemical component that matches the specified SMILES string using the
        specified query type.

        For details see:
        `Chemical Structure Search <http://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/chemSmiles.html>`_

        Parameters
        ----------
        smiles : str
           SMILES string representing chemical structure
        match_type : str
           One of the 5 supported types
        percentSimilarity : float
           percent similarity for similarity search. This parameter is ignored
           for all other query types [default: 0.0]
        '''

        max_rows = 1000

        query = ('{'
                   '"query": {'
                   '"type": "terminal",'
                   '"service": "chemical",'
                   '"parameters": {'
                     f'"value": "{smiles}",'
                     '"type": "descriptor",'
                     '"descriptor_type": "SMILES",'
                     f'"match_type": "{match_type}"'
                   '}'
                 '},'
                 '"return_type": "entry",'
                 '"request_options": {'
                   '"pager": {'
                   '"start": 0,'
                   f'"rows": {max_rows}'
                  '},'
                   '"scoring_strategy": "combined",'
                  '"sort": ['
                     '{'
                       '"sort_by": "score",'
                       '"direction": "desc"'
                     '}'
                   ']'
                  '}'
                '}'
                )

        result_type, identifiers, scores = post_query(query)

        self.structureIds = set()
        for identifier, score in zip(identifiers, scores):
            if (score*100.0 >= percentSimilarity):
                self.structureIds.add(identifier)

    def get_structure_ids(self):
        return list(self.structureIds)

    def __call__(self, t):
        match = t[0] in self.structureIds

        # If results are PDB IDs, but the keys contains chain names,
        # then trucate the chain name before matching (eg. 4HHB.A -> 4HHB)
        if not match and len(t[0]) > 4:
            match = t[0][:4] in self.structureIds

        return match
