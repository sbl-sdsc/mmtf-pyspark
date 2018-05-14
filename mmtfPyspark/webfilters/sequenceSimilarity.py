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

    BLAST = 'blast'
    PSI_BLAST = 'psi-blast'

    def __init__(self, sequence, searchTool=BLAST, eValueCutoff=10.0,
                 sequenceIdentityCutoff=0, maskLowComplexity=True):
        '''Filters by squence similarity using all default parameters.

        Parameters
        ----------
        sequence : str
           query sequence
        searchTool : class variable
           sequenceSimilarity.BLAST or sequenceSimilarity.PSI_BLAST
        eValueCutoff : float
           maximun e-value
        sequenceIdentityCutoff : int
           minimum sequence identity cutoff
        maskLowComplexity : bool
           if true, mask (ignore) low complexity regions in sequence
        '''

        if len(sequence) < 12:
            raise ValueError(
                "ERROR: the query sequence must be at least 12 residues long")

        complexity = 'yes' if maskLowComplexity else 'no'

        query = "<orgPdbQuery>" + "<queryType>org.pdb.query.simple.SequenceQuery</queryType>" \
                + "<sequence>" + sequence + "</sequence>" + "<searchTool>" + searchTool \
                + "</searchTool>" + "<maskLowComplexity>" + complexity\
                + "</maskLowComplexity>" + "<eValueCutoff>" + str(eValueCutoff) \
                + "</eValueCutoff>" + "<sequenceIdentityCutoff>" + str(sequenceIdentityCutoff) \
                + "</sequenceIdentityCutoff>" + "</orgPdbQuery>"

        self.filter = AdvancedQuery(query)

    def __call__(self, t):
        return self.filter(t)
