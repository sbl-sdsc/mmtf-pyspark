#!/user/bin/env python
'''structureToPolymerChain.py:

Maps a structure to its individual polymer chains. Polymer chains
include polypeptides, polynucleotides, and linear and branched polysaccharides.
For a multi-model structure, only the first model is considered.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "debug"
from mmtf.utils import *
from mmtf.api.mmtf_writer import MMTFEncoder
from mmtfPyspark.utils import MmtfStructure

class StructureToPolymerChains(object):
    '''Extracts all polymer chains from a structure. If the argument is set to true,
       the assigned key is: <PDB ID.Chain ID>, where Chain ID is the unique identifier
       assigned to each molecular entity in an mmCIF file. This Chain ID corresponds to
       `_atom_size.label_asym_id <http://mmcif.wwpdb.org/dictionaries/mmcif_mdb.dic/Items/_atom_site.label_asym_id.html>`_ 
       field in an mmCIF file.

    Attributes
    ----------
    useChainIdInsteadOfChainName : bool
       if true, use Chain Id in the key assignments
    excludeDuplicates : bool
       if true return only one chain for each sequence
    '''

    def __init__(self, useChainIdInsteadOfChainName = False, excludeDuplicates = False):
        self.useChainIdInsteadOfChainName = useChainIdInsteadOfChainName
        self.excludeDuplicates = excludeDuplicates


    def __call__(self, t):

        if type(t[1]) == MmtfStructure and t[1].alt_loc_set == False:
            structure = t[1].set_alt_loc_list()
        else:
            structure = t[1]

        # Precalculate indices
        numChains = structure.chains_per_model[0]
        chainToEntityIndex = self._get_chain_to_entity_index(structure)
        atomsPerChain, bondsPerChain = self._get_num_atoms_and_bonds(structure)

        chainList = list()
        seqSet = set()
        groupCounter = 0
        atomCounter = 0

        for i in range(numChains):
            polymerChain = MMTFEncoder()

            entityToChainIndex = chainToEntityIndex[i]

            chain_type = structure.entity_list[entityToChainIndex]['type']

            polymer = chain_type == "polymer"
            polymerAtomCount = 0

            atomMap = {}

            structureId = ''

            if polymer:
                # To avoid of information loss, add chainName/IDs and entity id
                # This required by some queries
                structureId = structure.structure_id + '.' +\
                              structure.chain_name_list[i] + '.' +\
                              structure.chain_id_list[i] + '.' +\
                              str(entityToChainIndex + 1)

                # Set header
                polymerChain.init_structure(bondsPerChain[i], atomsPerChain[i],
                    structure.groups_per_chain[i], 1, 1, structureId)
                decoder_utils.add_xtalographic_info(structure, polymerChain)
                decoder_utils.add_header_info(structure, polymerChain)

                # Set model info (only one model: 0)
                polymerChain.set_model_info(0,1)

                # Set entity and chain info
                polymerChain.set_entity_info([0],
                    structure.entity_list[entityToChainIndex]['sequence'],
                    structure.entity_list[entityToChainIndex]['description'],
                    structure.entity_list[entityToChainIndex]['type'])
                polymerChain.set_chain_info(structure.chain_id_list[i],
                    structure.chain_name_list[i],
                    structure.groups_per_chain[i])

            for j in range(structure.groups_per_chain[i]):
                groupIndex = structure.group_type_list[groupCounter]

                if polymer:
                    # Set group info
                    polymerChain.set_group_info(structure.group_list[groupIndex]['groupName'],
                        structure.group_id_list[groupCounter],
                        structure.ins_code_list[groupCounter],
                        structure.group_list[groupIndex]['chemCompType'],
                        len(structure.group_list[groupIndex]['atomNameList']),
                        len(structure.group_list[groupIndex]['bondOrderList']),
                        structure.group_list[groupIndex]['singleLetterCode'],
                        structure.sequence_index_list[groupCounter],
                        structure.sec_struct_list[groupCounter])

                for k in range(len(structure.group_list[groupIndex]['atomNameList'])):
                    if polymer:
                        atomMap[atomCounter] = polymerAtomCount
                        polymerAtomCount += 1

                        polymerChain.set_atom_info(
                            structure.group_list[groupIndex]['atomNameList'][k],
                            structure.atom_id_list[atomCounter],
                            structure.alt_loc_list[atomCounter],
                            structure.x_coord_list[atomCounter],
                            structure.y_coord_list[atomCounter],
                            structure.z_coord_list[atomCounter],
                            structure.occupancy_list[atomCounter],
                            structure.b_factor_list[atomCounter],
                            structure.group_list[groupIndex]['elementList'][k],
                            structure.group_list[groupIndex]['formalChargeList'][k],)

                    atomCounter += 1

                if polymer:
                    # Add intra-group bond info
                    for l in range(len(structure.group_list[groupIndex]['bondOrderList'])):
                        bondIndOne = structure.group_list[groupIndex]['bondAtomList'][l*2]
                        bondIndTwo = structure.group_list[groupIndex]['bondAtomList'][l*2+1]
                        bondOrder = structure.group_list[groupIndex]['bondOrderList'][l]

                        polymerChain.set_group_bond(bondIndOne, bondIndTwo, bondOrder)

                groupCounter += 1

            if polymer:
                # TODO skipping adding inter group bond info for now

                polymerChain.finalize_structure()

                chId = structure.chain_name_list[i]
                if self.useChainIdInsteadOfChainName :
                    chId = structure.chain_id_list[i]
                if self.excludeDuplicates:
                    if chainToEntityIndex[i] in seqSet:
                        continue
                    seqSet.add(chainToEntityIndex[i])
                chainList.append((structure.structure_id + "." + chId, polymerChain))

        return chainList


    def _get_num_atoms_and_bonds(self, structure):
        '''Gets the number of atoms and bonds per chain
        '''
        numChains = structure.chains_per_model[0]
        atomsPerChain = [0] * numChains
        bondsPerChain = [0] * numChains
        groupCounter = 0

        for i in range(numChains):

            for j in range(structure.groups_per_chain[i]):
                groupIndex = structure.group_type_list[groupCounter]
                atomsPerChain[i] += len(structure.group_list[groupIndex]['atomNameList'])
                bondsPerChain[i] += len(structure.group_list[groupIndex]['bondOrderList'])
                groupCounter += 1

        return atomsPerChain, bondsPerChain


    def _get_chain_to_entity_index(self, structure):
        '''Returns an list that maps a chain index to an entity index.

        Parameters
        ----------
        structure : structureDataInterFace
        '''
        entityChainIndex = [0] * structure.num_chains

        for i in range(len(structure.entity_list)):

            for j in structure.entity_list[i]["chainIndexList"]:
                entityChainIndex[j] = i

        return entityChainIndex
