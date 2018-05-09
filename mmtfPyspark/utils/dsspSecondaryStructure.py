#!/user/bin/env python
'''dsspSecondaryStructure.py:
'''
__author__ = "Yue Yu"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"


from enum import Enum


class DsspSecondaryStructure(Enum):

    PI_HELIX = 0
    BEND = 1
    ALPHA_HELIX = 2
    EXTENDED = 3
    THREE_TEN_HELIX = 4
    BRIDGE = 5
    TURN = 6
    COIL = 7

    def get_q3_code(numericCode):

        cases = {0: DsspSecondaryStructure.ALPHA_HELIX,
                 1: DsspSecondaryStructure.COIL,
                 2: DsspSecondaryStructure.ALPHA_HELIX,
                 3: DsspSecondaryStructure.EXTENDED,
                 4: DsspSecondaryStructure.ALPHA_HELIX,
                 5: DsspSecondaryStructure.EXTENDED,
                 6: DsspSecondaryStructure.COIL,
                 7: DsspSecondaryStructure.COIL
                 }
        if numericCode in cases:
            return cases[numericCode]
        else:
            return DsspSecondaryStructure.COIL

    def get_one_letter_code(self):
        cases = {0: '5',
                 1: 'S',
                 2: 'H',
                 3: 'E',
                 4: 'G',
                 5: 'B',
                 6: 'T',
                 7: 'C'
                 }
        return cases[self.value]

    def get_dssp_code(numericCode):
        for x in list(DsspSecondaryStructure):
            if x.value == numericCode:
                return x
        return DsspSecondaryStructure.COIL
