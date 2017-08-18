#!/user/bin/env python
'''
dsspSecondaryStructure.py:

Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
'''

class dsspSecondaryStructure(object):

    def getQ3Code(numericCode):

        cases = { 0 : "ALPHA_HELIX",
                  1 : "COIL",
                  2 : "ALPHA_HELIX",
                  3 : "EXTENDED",
                  4 : "ALPHA_HELIX",
                  5 : "EXTENDED",
                  6 : "COIL",
                  7 : "COIL"
                  }
        if numericCode in cases:
            return cases[numericCode]

        else:
            return "COIL"
