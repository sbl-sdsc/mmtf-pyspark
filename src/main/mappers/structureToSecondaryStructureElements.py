#!/user/bin/env python
'''
structureToSecondaryStructureElements.py:

Authorship information:
    __author__ = "Yue Yu"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"

'''
from pyspark.sql import Row

class structureToSecondaryStructureElements(object):
    '''Maps chain sequences to it's sequence elements

    Attributes:
    '''
    def __init__(self, label, length = 4):
        '''Constructor sets the segment length.

        '''
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




