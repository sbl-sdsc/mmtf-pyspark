#!/user/bin/env python
'''
dsspSecondaryStructure.py:

Authorship information:
    __author__ = "Yue Yu"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from enum import Enum

class dsspSecondaryStructure(Enum):

    PI_HELIX = 0
    BEND = 1
    ALPHA_HELIX = 2
    EXTENDED = 3
    THREE_TEN_HELIX = 4
    BRIDGE = 5
    TURN = 6
    COIL = 7

    def getQ3Code(numericCode):

        cases = { 0 : dsspSecondaryStructure.ALPHA_HELIX,
                  1 : dsspSecondaryStructure.COIL,
                  2 : dsspSecondaryStructure.ALPHA_HELIX,
                  3 : dsspSecondaryStructure.EXTENDED,
                  4 : dsspSecondaryStructure.ALPHA_HELIX,
                  5 : dsspSecondaryStructure.EXTENDED,
                  6 : dsspSecondaryStructure.COIL,
                  7 : dsspSecondaryStructure.COIL
                  }
        if numericCode in cases:
            return cases[numericCode]

        else:
            return dsspSecondaryStructure.COIL


    def getOneLetterCode(self):
        cases = { 0 : '5',
                  1 : 'S',
                  2 : 'H',
                  3 : 'E',
                  4 : 'G',
                  5 : 'B',
                  6 : 'T',
                  7 : 'C'
                  }
        return cases[self.value]


    def getDsspCode(numericCode):
        for x in list(dsspSecondaryStructure):
            if x.value == numericCode:
                return x
        return dsspSecondaryStructure.COIL
