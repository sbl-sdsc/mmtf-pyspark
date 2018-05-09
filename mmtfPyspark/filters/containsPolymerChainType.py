#!/user/bin/env python
'''containsPolymerChainType.py

This filter returns entries that contain chains made of the specified
monomer types. The default constructor returns entries that contain at least
one chain that matches the conditions. If the "exclusive" flag is set to true
in the constructor, all chains must match the conditions. For a multi-model
structure, this filter only checks the first model.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"


class ContainsPolymerChainType(object):
    '''Default constructor matches any entry that contains a chain with only
    the specified monomer type

    Attributes
    ----------
    monomer_type : list
       list of monomer types in a polymer chain

    '''

    D_PEPTIDE_COOH_CARBOXY_TERMINUS = "D-PEPTIDE COOH CARBOXY TERMINUS"
    D_PEPTIDE_NH3_AMINO_TERMINUS = "D-PEPTIDE NH3 AMINO TERMINUS"
    D_PEPTIDE_LINKING = "D-PEPTIDE LINKING"
    D_SACCHARIDE = "D-SACCHARIDE"
    D_SACCHARIDE_14_and_14_LINKING = "D-SACCHARIDE 1,4 AND 1,4 LINKING"
    D_SACCHARIDE_14_and_16_LINKING = "D-SACCHARIDE 1,4 AND 1,6 LINKING"
    DNA_OH_3_PRIME_TERMINUS = "DNA OH 3 PRIME TERMINUS"
    DNA_OH_5_PRIME_TERMINUS = "DNA OH 5 PRIME TERMINUS"
    DNA_LINKING = "DNA LINKING"
    L_PEPTIDE_COOH_CARBOXY_TERMINUS = "L-PEPTIDE COOH CARBOXY TERMINUS"
    L_PEPTIDE_NH3_AMINO_TERMINUS = "L-PEPTIDE NH3 AMINO TERMINUS"
    L_PEPTIDE_LINKING = "L-PEPTIDE LINKING"
    L_SACCHARIDE = "L-SACCHARIDE"
    L_SACCHARIDE_14_AND_14_LINKING = "L-SACCHARDIE 1,4 AND 1,4 LINKING"
    L_SACCHARIDE_14_AND_16_LINKING = "L-SACCHARIDE 1,4 AND 1,6 LINKING"
    PEPTIDE_LINKING = "PEPTIDE LINKING"
    RNA_OH_3_PRIME_TERMINUS = "RNA OH 3 PRIME TERMINUS"
    RNA_OH_5_PRIME_TERMINUS = "RNA OH 5 PRIME TERMINUS"
    RNA_LINKING = "RNA LINKING"
    NON_POLYMER = "NON-POLYMER"
    OTHER = "OTHER"
    SACCHARIDE = "SACCHARIDE"

    def __init__(self, monomer_type, exclusive=False):
        if type(monomer_type) == str:
            monomer_type = monomer_type.split(',')

        self.exclusive = exclusive
        self.monomer_type = monomer_type

    def __call__(self, t):
        structure = t[1]
        contrains_polymer = False
        global_match = False
        # get number of chains in first model, nessary?
        num_chains = structure.chains_per_model[0]
        group_counter = 0

        for i in range(num_chains):
            match = True
            chain_type = [chain['type'] for chain in structure.entity_list
                          if i in chain['chainIndexList']][0]
            polymer = chain_type == "polymer"

            if polymer:
                contains_polymer = True
            else:
                match = False

            for j in range(structure.groups_per_chain[i]):
                if match and polymer:
                    group_idx = structure.group_type_list[group_counter]
                    group_type = structure.group_list[group_idx]['chemCompType']
                    match = (group_type in self.monomer_type)
                group_counter += 1

            if (polymer and match and not self.exclusive):
                return True

            if (polymer and not match and self.exclusive):
                return False

            if match:
                global_match = True

        return global_match and contains_polymer
