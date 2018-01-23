#!/user/bin/env python
'''
structureToBiopython.py:

Maps a structure to BioPython Structure

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "debug"
'''
from Bio.PDB.mmtf import DefaultParser
from mmtf.utils import decoder_utils

# TODO MMTFEncoder error

class structureToBiopython(object):

    def __call__(self, t):

        parser = DefaultParser.StructureDecoder()
        t.pass_data_on(parser)

        return parser.structure_bulder.get_structure()
