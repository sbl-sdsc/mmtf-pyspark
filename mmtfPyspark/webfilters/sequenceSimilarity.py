#!/user/bin/env python
'''sequenceSimilarity.py

This filter returns entries that pass the sequence similarity search
criteria. Searches protein and nucleic acid sequences using the BLAST.
PSI-BLAST is used to find more distantly related protein sequences.

The E value, or Expect value, is a parameter that describes the number of
hits one can expect to see just by chance when searching a database of a
particular size. For example, an E value of one indicates that a result will
contain one sequence with similar score simply by chance. The scoring takes
chain length into consideration and therefore shorter sequences can have
identical matches with high E value.

The Low Complexity filter masks low complexity regions in a sequence to
filter out avoid spurious alignments.

Sequence Identity Cutoff (%) filter removes entries of low sequence
similarity. The cutoff value is a percentage value between 0 to 100.

Note: sequences must be at least 12 residues long. For shorter sequences try
the Sequence Motif Search.

References
----------
- BLAST: BLAST: Sequence searching using NCBI's BLAST (Basic Local Alignment
  Search Tool) Program , Altschul, S.F., Gish, W., Miller, W., Myers, E.W.
  and Lipman, D.J. Basic local alignment search tool. J. Mol. Biol. 215:
  403-410 (1990)

- PSI-BLAST: Sequence searching to detect distantly related evolutionary
  relationships using NCBI's PSI-BLAST (Position-Specific Iterated BLAST).
'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from mmtfPyspark.webfilters import AdvancedQuery


class SequenceSimilarity(object):

    def __init__(self, sequence, target="Protein", evalue_cutoff=0.1, identity_cutoff=0):
        '''Filters by squence similarity using all default parameters.

        Parameters
        ----------
        sequence : str
           query sequence
        target : str
           Protein, DNA, or RNA
        eValueCutoff : float
           maximun e-value
        sequenceIdentityCutoff : int
           minimum sequence identity cutoff
        '''

        if len(sequence) < 20:
            raise ValueError(
                "ERROR: the query sequence must be at least 20 residues long")

        targets = {'Protein: 'pdb_protein_sequence', 'DNA': 'pdb_dna_sequence', 'RNA': 'pdb_rna_sequence'}

        target_type = targets.get(target)

        max_rows = 1000

        query = ('{'
                   '"query": {'
                   '"type": "terminal",'
                   '"service": "sequence",'
                   '"parameters": {'
                     f'"evalue_cutoff": "{evalue_cutoff}",'
                     f'"identity_cutoff": "{identity_cutoff}",'
                     f'"target": "{target}",'
                     f'"value": "{sequence}",'
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

        self.structureIds = set(identifiers)

    def __call__(self, t):
        match = t[0] in self.structureIds

        # If results are PDB IDs, but the keys contains chain names,
        # then trucate the chain name before matching (eg. 4HHB.A -> 4HHB)
        if not match and not self.chainLevel and len(t[0]) > 4:
            match = t[0][:4] in self.structureIds
