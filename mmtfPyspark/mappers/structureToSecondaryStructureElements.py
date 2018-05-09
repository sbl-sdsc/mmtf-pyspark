#!/user/bin/env python
'''structureToSecondaryStructureElements.py:

Maps chain sequences to its sequence segments.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"
from pyspark.sql import Row


class StructureToSecondaryStructureElements(object):
    '''Constructor sets the segment length.

    Attributes
    ----------
    label : str
       label of the structure
    length : int
       segment length [4]
    '''

    def __init__(self, label, length=4):
        self.label = label
        self.length = length

    def __call__(self, t):
        sequence = t[1]
        dsspQ3 = t[6]
        sequences = []

        i = 0
        while i < len(sequence):
            currLength = 0
            currSequence = ""

            for j in range(i, len(sequence)):

                if dsspQ3[j:j + 1] == self.label:
                    currLength += 1
                    currSequence += sequence[j: j + 1]
                else:
                    break

            i += currLength + 1

            if currLength >= self.length:
                sequences.append(Row(currSequence, self.label))

            i += 1

        return sequences
