#!/user/bin/env python
'''secondaryStructure.py

This filter returns entries that contain polymer chain(s) with the specified
fraction of secondary structure assignments, obtained by DSSP. Note, DSSP
secondary structure in MMTF files is assigned by the BioJava implementation of
DSSP. It may differ in some cases from the original DSSP implementation.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.utils import DsspSecondaryStructure


class SecondaryStructure(object):
    '''The default constructor returns entries that contain at least one
    polymer chain that matches the criteria. If the "exclusive" flag is set to
    true in the constructor, all polymer chains must match the criteria. For a multi-model
    structure, this filter only checks the first model.

    Attributes
    ----------
    helixFractionMin : float
       minimum value for helix fraction [default: 0.0]
    helixFractionMax : float
       maximum value for helix fraction [default: 1.0]
    sheetFractionMin : float
       minimum value for sheet fraction [default: 0.0]
    sheetFractionMax : float
       maximum value for sheet fraction [default: 1.0]
    coilFractionMin : float
       minimum value for coil fractions [default: 0.0]
    coilFractionMax : float
       maximum value for coil fractions [default: 1.0]
    exclusive : bool
       exclusive flag [False]
    '''

    def __init__(self, helixFractionMin=0.0, helixFractionMax=1.0,
                 sheetFractionMin=0.0, sheetFractionMax=1.0,
                 coilFractionMin=0.0, coilFractionMax=1.0, exclusive=False):

        self.helixFractionMax = helixFractionMax
        self.helixFractionMin = helixFractionMin
        self.sheetFractionMax = sheetFractionMax
        self.sheetFractionMin = sheetFractionMin
        self.coilFractionMax = coilFractionMax
        self.coilFractionMin = coilFractionMin
        self.exclusive = exclusive

    def __call__(self, t):
        structure = t[1]
        contains_polymer = False
        global_match = False
        num_chains = structure.chains_per_model[0]
        sec_struct = structure.sec_struct_list
        group_counter = 0

        for i in range(num_chains):
            helix = 0.0
            sheet = 0.0
            coil = 0.0
            other = 0.0
            match = True

            chain_type = [chain['type'] for chain in structure.entity_list
                          if i in chain['chainIndexList']][0]
            polymer = chain_type == 'polymer'

            if polymer:
                contains_polymer = True
            else:
                match = False

            for j in range(structure.groups_per_chain[i]):

                if match and polymer:
                    code = sec_struct[group_counter]
                    secondary_structure = DsspSecondaryStructure.get_q3_code(
                        code)

                    if secondary_structure == DsspSecondaryStructure.ALPHA_HELIX:
                        helix += 1
                    elif secondary_structure == DsspSecondaryStructure.EXTENDED:
                        sheet += 1
                    elif secondary_structure == DsspSecondaryStructure.COIL:
                        coil += 1
                    else:
                        other += 1

                group_counter += 1

            if match and polymer:
                n = structure.groups_per_chain[i] - other
                helix /= n
                sheet /= n
                coil /= n

                match = helix >= self.helixFractionMin and \
                    helix <= self.helixFractionMax and \
                    sheet >= self.sheetFractionMin and \
                    sheet <= self.sheetFractionMax and \
                    coil >= self.coilFractionMin and \
                    coil <= self.coilFractionMax

            if (polymer and match and not self.exclusive):
                return True

            if (polymer and not match and self.exclusive):
                return False

            if match:
                global_match = True

        return global_match and contains_polymer
