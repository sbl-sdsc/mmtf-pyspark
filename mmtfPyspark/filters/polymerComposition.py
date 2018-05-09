#!/user/bin/env python
'''polymerComposition.py

This filter returns entries that contain chains made of the specified
monomer types.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"


class PolymerComposition(object):
    '''The default constructor returns entries that contain at least
    one chain that matches the conditions. If the "exclusive" flag is set to true
    in the constructor, all chains must match the conditions. For a multi-model
    structure, this filter only checks the first model.

    Optional constructor that can be used to filter entries that exclusively
    match all chains.

    Attributes
    ----------
    exclusive : bool
       if true, all chains must be made of the specified monomers
    '''

    # define sets of residue types eg:
    # 20 nat. amino acids, 22 nat. amino acids, nat. DnNA, nat. RNA
    # custom sets
    AMINO_ACIDS_20 = ["ALA", "ARG", "ASN", "ASP", "CYS", "GLN", "GLU", "GLY", "HIS",
                      "ILE", "LEU", "LYS", "MET", "PHE", "PRO", "SER", "THR", "TRP", "TYR", "VAL"]
    AMINO_ACIDS_22 = ["ALA", "ARG", "ASN", "ASP", "CYS", "GLN", "GLU", "GLY", "HIS", "ILE",
                      "LEU", "LYS", "MET", "PHE", "PRO", "SER", "THR", "TRP", "TYR", "VAL", "SEC", "PYL"]
    DNA_STD_NUCLEOTIDES = ["DA", "DC", "DG", "DT"]
    RNA_STD_NUCLEOTIDES = ["A", "C", "G", "U"]

    def __init__(self, monomer_type, exclusive=False):
        if type(monomer_type) == str:
            monomer_type = monomer_type.split(",")

        self.exclusive = exclusive
        self.residues = monomer_type

    def __call__(self, t):
        structure = t[1]
        contains_polymer = False
        global_match = False
        # get number of chains in first model, nessary?
        num_chains = structure.chains_per_model[0]
        # chain types in entity as key, enetity from entity_list
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
            # group_type_list

            for j in range(structure.groups_per_chain[i]):
                if match and polymer:
                    group_idx = structure.group_type_list[group_counter]
                    group_type = structure.group_list[group_idx]['groupName']
                    match = (group_type in self.residues)
                group_counter += 1

            if (polymer and match and not self.exclusive):
                return True

            if (polymer and not match and self.exclusive):
                return False

            if match:
                global_match = True

        return global_match and contains_polymer
