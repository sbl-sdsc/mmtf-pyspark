#!/user/bin/env python
'''structureToSecondaryStructureSegments.py:

Maps chain seuqnce to its sequence segments

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"
from pyspark.sql import Row


class StructureToSecondaryStructureSegments(object):
    '''Constructor sets the segment length.

    Attributes
    ----------
    length : int
       length of the secondary structure segments
    '''

    def __init__(self, length):
        if length % 2 != 1:
            print("length has to be an odd number")

        self.length = length

    def __call__(self, t):
        structureChainId = t[0]
        sequence = t[1]
        dsspQ3 = t[6]
        dsspQ8 = t[5]

        numSegments = max(0, len(sequence) - self.length)
        sequences = []

        for i in range(len(sequence) - self.length):
            currSeq = sequence[i:i + self.length]

            # print(dsspQ3)
            labelQ3 = dsspQ3[i + int(self.length / 2): i + int(self.length / 2) + 1]
            labelQ8 = dsspQ8[i + int(self.length / 2): i + int(self.length / 2) + 1]

            if (labelQ8 != "X" and labelQ3 != "X"):
                sequences.append(
                    Row(structureChainId, currSeq, labelQ8, labelQ3))

        return sequences
