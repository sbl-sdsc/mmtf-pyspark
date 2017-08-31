#!/user/bin/env python
'''
structureToSecondaryStructureElements.py:

Maps a structure to its polypeptides, polynucleotides chain sequences.
For a multi-model structure, only the first model is considered.

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
'''
from pyspark.sql import Row

class structureToSecondaryStructureElements(object):
    '''
    Extracts all polymer chains from a structure. If the argument is set to true,
	the assigned key is: <PDB ID.Chain ID>, where Chain ID is the unique identifier
	assigned to each molecular entity in an mmCIF file. This Chain ID corresponds to
	<a href="http://mmcif.wwpdb.org/dictionaries/mmcif_mdb.dic/Items/_atom_site.label_asym_id.html">
	_atom_site.label_asym_id</a> field in an mmCIF file.

    Attributes:
        useChainIdInsteadOfChainName (bool): if true, use the Chain Id in the key assignments
        duplicateSequences (bool): if true return only one chain for each unique sequence
    '''
    def __init__(self, label, length = 4):
        self.label = label
        self.length = length


    def __call__(self, t):
        # TODO double check indexing
        sequence = t[1]
        dsspQ3 = t[6]
        sequences = []

        i = 0
        # TODO double check while loop logic
        while i < len(sequence):
            currLength = 0
            currSequence = ""

            for j in range(i, len(sequence)):

                if dsspQ3[j:j+1] == self.label:
                    currLength = 0
                    currSequence += sequence[j: j+1]
                else: break

            i += currLength + 1

            if currLength >= self.length:
                sequences.append(Row(currSequence, self.label))

        return sequences
