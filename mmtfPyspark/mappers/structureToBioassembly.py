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
from numpy import *


class StructureToBioassembly(object):
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

    def __init__(self, useChainIdInsteadOfChainName=False, excludeDuplicates=False):
        self.useChainIdInsteadOfChainName = useChainIdInsteadOfChainName
        self.excludeDuplicates = excludeDuplicates

    def __call__(self, t):
        structure = t[1]
        if not structure.alt_loc_set:
            structure = structure.set_alt_loc_list()
        # print(structure.group_type_list)

        bioassemblies = structure.bio_assembly

        numBioassembly = len(bioassemblies)

        resList = list()

        for i in range(numBioassembly):
            bioAssembly = MMTFEncoder()
            structureId = structure.structure_id + '-BioAssembly' + \
                bioassemblies[i]['name']
            totAtoms = 0
            totBonds = 0
            totGroups = 0
            totChains = 0
            totModels = structure.num_models
            numTrans = len(bioassemblies[i]['transformList'])
            bioChainList = [[]] * numTrans
            transMatrix = [[]] * numTrans
            for ii in range(numTrans):

                bioChainList[ii] = bioassemblies[i]['transformList'][ii]['chainIndexList']
                transMatrix[ii] = bioassemblies[i]['transformList'][ii]['matrix']

                for j in range(totModels):
                    totChains = totChains + len(bioChainList[ii])
                    groupCounter = 0
                    for k in range(structure.chains_per_model[j]):

                        adding = False
                        for currChain in bioChainList[ii]:
                            if currChain == k:
                                adding = True
                        if adding:
                            totGroups = totGroups + \
                                structure.groups_per_chain[k]
                        for h in range(structure.groups_per_chain[k]):
                            if adding:
                                groupIndex = structure.group_type_list[groupCounter]
                                totAtoms = totAtoms + \
                                    len(structure.group_list[groupIndex]
                                        ['atomNameList'])
                                totBonds = totBonds + \
                                    len(structure.group_list[groupIndex]
                                        ['bondOrderList'])
                            groupCounter = groupCounter + 1
            # Set header
            bioAssembly.init_structure(
                totBonds, totAtoms, totGroups, totChains, totModels, structureId)
            decoder_utils.add_xtalographic_info(structure, bioAssembly)
            decoder_utils.add_header_info(structure, bioAssembly)

            modelIndex = 0
            chainIndex = 0
            groupIndex = 0
            atomIndex = 0
            chainCounter = 0
            description = {}

            for ii in range(totModels):
                numChainsPerModel = structure.chains_per_model[modelIndex] * numTrans
                bioAssembly.set_model_info(modelIndex, numChainsPerModel)
                chainToEntityIndex = self._getChainToEntityIndex(structure)

                for j in range(structure.chains_per_model[modelIndex]):
                    currGroupIndex = groupIndex
                    currAtomIndex = atomIndex
                    for k in range(numTrans):
                        currChainList = bioChainList[k]
                        currMatrix = transMatrix[k]
                        addThisChain = False
                        for currChain in currChainList:
                            if currChain == j:
                                addThisChain = True
                        groupIndex = currGroupIndex
                        atomIndex = currAtomIndex
                        xCoords = structure.x_coord_list
                        yCoords = structure.y_coord_list
                        zCoords = structure.z_coord_list

                        m = reshape(matrix(currMatrix), (4, 4))

                        if addThisChain:
                            entityToChainIndex = chainToEntityIndex[chainIndex]
                            if structure.entity_list[entityToChainIndex]['description'] in description:
                                index = description[structure.entity_list[entityToChainIndex]['description']]
                                bioAssembly.entity_list[index]['chainIndexList'].append(chainCounter)
                            else:
                                bioAssembly.set_entity_info([chainCounter],
                                                            structure.entity_list[entityToChainIndex]['sequence'],
                                                            structure.entity_list[entityToChainIndex]['description'],
                                                            structure.entity_list[entityToChainIndex]['type'])
                                description[bioAssembly.entity_list[-1]['description']] = len(bioAssembly.entity_list)-1
                            bioAssembly.set_chain_info(structure.chain_id_list[chainIndex],
                                                       structure.chain_name_list[chainIndex],
                                                       structure.groups_per_chain[chainIndex])
                            chainCounter = chainCounter + 1
                        for jj in range(structure.groups_per_chain[chainIndex]):
                            # print(structure.group_type_list)
                            currgroup = structure.group_type_list[groupIndex]
                            # if ii == 0 and j == 0 and jj < 10:
                            # print(currgroup)

                            if addThisChain:
                                bioAssembly.set_group_info(structure.group_list[currgroup]['groupName'],
                                                           structure.group_id_list[groupIndex],
                                                           structure.ins_code_list[groupIndex],
                                                           structure.group_list[currgroup]['chemCompType'],
                                                           len(
                                                               structure.group_list[currgroup]['atomNameList']),
                                                           len(
                                                               structure.group_list[currgroup]['bondOrderList']),
                                                           structure.group_list[currgroup]['singleLetterCode'],
                                                           structure.sequence_index_list[groupIndex],
                                                           structure.sec_struct_list[groupIndex])
                            for kk in range(len(structure.group_list[currgroup]['atomNameList'])):
                                if addThisChain:
                                    p1 = array(
                                        [xCoords[atomIndex], yCoords[atomIndex], zCoords[atomIndex], 1])
                                    p2 = matmul(p1, m)
                                    bioAssembly.set_atom_info(
                                        structure.group_list[currgroup]['atomNameList'][kk],
                                        structure.atom_id_list[atomIndex],
                                        structure.alt_loc_list[atomIndex],
                                        p2.item(0),
                                        p2.item(1),
                                        p2.item(2),
                                        structure.occupancy_list[atomIndex],
                                        structure.b_factor_list[atomIndex],
                                        structure.group_list[currgroup]['elementList'][kk],
                                        structure.group_list[currgroup]['formalChargeList'][kk], )

                                atomIndex = atomIndex + 1

                            # bond not implemented
                            if addThisChain:

                                for l in range(len(structure.group_list[currgroup]['bondOrderList'])):

                                    bondIndOne = structure.group_list[currgroup]['bondAtomList'][l * 2]
                                    bondIndTwo = structure.group_list[currgroup]['bondAtomList'][l * 2 + 1]
                                    bondOrder = structure.group_list[currgroup]['bondOrderList'][l]

                                    #newChain.set_group_bond(bondIndOne, bondIndTwo, bondOrder)

                                    bioAssembly.current_group.bond_atom_list += [
                                        bondIndOne, bondIndTwo]
                                    bioAssembly.current_group.bond_order_list.append(
                                        bondOrder)

                            groupIndex = groupIndex + 1

                    chainIndex = chainIndex + 1
                modelIndex = modelIndex + 1
                # print(type(currMatrix))
            bioAssembly.finalize_structure()
            resList.append((structureId, bioAssembly))
        return resList

    def _getNumAtomsAndBonds(self, structure):
        '''Gets the number of atoms and bonds per chain
        '''
        numChains = structure.chains_per_model[0]
        atomsPerChain = [0] * numChains
        bondsPerChain = [0] * numChains
        groupCounter = 0

        for i in range(numChains):

            for j in range(structure.groups_per_chain[i]):
                groupIndex = structure.group_type_list[groupCounter]
                atomsPerChain[i] = len(
                    structure.group_list[groupIndex]['atomNameList'])
                bondsPerChain[i] = len(
                    structure.group_list[groupIndex]['bondOrderList'])
                groupCounter += 1

        return atomsPerChain, bondsPerChain

    def _getChainToEntityIndex(self, structure):
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
