#!/user/bin/env python
'''
structureToSecondaryStructureSegments.py:

Maps chain seuqnce to its sequence segments

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
'''
from pyspark.sql import Row

class structureToSecondaryStructureSegments(object):
    def __init__(self, length):
        '''
        Constructor sets the segment length.
        '''
        if length % 2 != 1:
            print("length has to be an odd number")

        self.length = length


    def __call__(self, t):
        # TODO double check indexing
        structureChainId = t[0]
        sequence = t[1]
        dsspQ3 = t[6]
        dsspQ8 = t[5]

        numSegments = max(0, len(sequence) - self.length)
        sequences = []

        for i in range(len(sequence)):
            currSeq = sequence[i:i+self.length]
            labelQ3 = dsspQ3[i+ int(self.length/2), i + int(self.length/2) + 1]
            labelQ8 = dsspQ8[i+ int(self.length/2), i + int(self.length/2) + 1]

            if (labelQ8 != "X" and labelQ3 != "X"):
                sequences.append(Row(structureChainId, currSeq, labelQ8, labelQ3))

        return sequences
