#!/user/bin/env python
'''structureToPolymerSequences.py:

This mapper maps a structure to it's polypeptides, polynucleotide chain sequences.
For a multi-model structure, only the first model is considered.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"


class StructureToPolymerSequences(object):
    '''This mapper maps a structure to it's polypeptides, polynucleotide chain sequences.
    For a multi-model structure, only the first model is considered.
    '''

    def __init__(self, useChainIdInsteadOfChainName=False, excludeDuplicates=False):
        '''Extracts all polymer chains from a structure. If the argument is set to true,
        the assigned key is: <PDB ID.Chain ID>, where Chain ID is the unique identifier
        assigned to each molecular entity in an mmCIF file. This Chain ID corresponds to
        `_atom_site.label_asym_id <http://mmcif.wwpdb.org/dictionaries/mmcif_mdb.dic/Items/_atom_site.label_asym_id.html>`_ 
        field in an mmCIF file.

        Parameters
        ----------
        useChainIdInsteadOfChainName : bool
           if true, use the Chain Id in the key assignments
        excludeDuplicates : bool 
           if true, return only one chain for each unique sequence= t[1]
        '''
        self.useChainIdInsteadOfChainName = useChainIdInsteadOfChainName
        self.excludeDuplicates = excludeDuplicates

    def __call__(self, t):
        structure = t[1]
        sequences = list()
        seqSet = set()

        chainToEntityIndex = self._get_chain_to_entity_index(structure)

        for i in range(structure.chains_per_model[0]):
            polymer = structure.entity_list[chainToEntityIndex[i]]['type'] == 'polymer'

            if polymer:
                key = t[0]

                if '.' in key:
                    key = key.split('.')[0]

                key += '.'
                if self.useChainIdInsteadOfChainName:
                    key += structure.chain_id_list[i]
                else:
                    key += structure.chain_name_list[i]

                if self.excludeDuplicates:
                    if chainToEntityIndex[i] in seqSet:
                        continue
                    seqSet.add(chainToEntityIndex[i])

                sequences.append(
                    (key, structure.entity_list[chainToEntityIndex[i]]['sequence']))

        return sequences

    def _get_chain_to_entity_index(self, structure):
        entityChainIndex = [0] * structure.num_chains

        for i in range(len(structure.entity_list)):

            for j in structure.entity_list[i]['chainIndexList']:
                entityChainIndex[j] = i

        return entityChainIndex
