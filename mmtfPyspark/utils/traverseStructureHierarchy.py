#!/usr/bin/env python
'''traverseStructureHierachy.py

A class that prints of hierachy information about a structure

Examples
--------
>>> pdb = mmtfReader.download_mmtf_files(['1STP'], sc)
>>> pdb.foreach(lambda t: traverseStructureHierarchy.printMmtfInfo(t[1]) )

>>> structure = mmtfReader.download_mmtf_files(['1STP'], sc).collect()[0]
>>> traverseStructureHierarchy.print_mmtf_info(structure[1])

'''
__author__ = "Yue Yu"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.utils.dsspSecondaryStructure import *

def _get_chain_to_entity_index(structure):
    entityChainIndex = [0] * structure.num_chains
    for i in range(0, len(structure.entity_list)):
        for j in structure.entity_list[i]["chainIndexList"]:
            entityChainIndex[j] = i
    return entityChainIndex


def _list_to_string(temp):
    return("[" + ", ".join(map(str, temp)) + "]")

def _check_structure_or_tuple(structure):
    if type(structure) == tuple:
        structure = structure[1]
    return structure


def print_mmtf_info(structure):
    structure = _check_structure_or_tuple(structure)

    print("*** STRUCTURE INFO ***")
    print("MmtfProducer    : " + structure.mmtf_producer)
    print("MmtfVersion     : " + structure.mmtf_version)
    print();

def print_metadata(structure):
    structure = _check_structure_or_tuple(structure)

    print("*** METADATA ***")
    print("StructureId           : " + structure.structure_id)
    print("Title                 : " + structure.title)
    print("Deposition date       : " + structure.deposition_date)
    print("Release date          : " + structure.release_date)
    print("Experimental method(s): " + _list_to_string(structure.experimental_methods))
    print("Resolution            : " + str(structure.resolution))
    print("Rfree                 : " + str(structure.r_free))
    print("Rwork                 : " + str(structure.r_work))
    print()


def print_crystallographic_data(structure):
    structure = _check_structure_or_tuple(structure)

    print("*** CRYSTALLOGRAPHIC DATA ***")
    print("Space group           : " + structure.space_group)
    print("Unit cell dimensions  : " + _list_to_string(["%0.2f" % i for i in structure.unit_cell]))
    print()

def print_bioassembly_data(structure):
    structure = _check_structure_or_tuple(structure)

    print("*** BIOASSEMBLY DATA ***")
    print("Number bioassemblies: " + str(len(structure.bio_assembly)))
    for i in range(0, len(structure.bio_assembly)):
        print("bioassembly: " + structure.bio_assembly[i]["name"])
        transformations = structure.bio_assembly[i]["transformList"]
        print("  Number transformations: " + str(len(transformations)))
        for j in range(0, len(transformations)):
            print("    transformation: " + str(j))
            print("    chains:         " + str(transformations[j]["chainIndexList"]))
            print("    rotTransMatrix: " + str(transformations[j]["matrix"]))

def print_entity_info(structure):
    '''Prints information about unique entities (molecules) in a structure
    '''
    print("*** ENTITY DATA ***")

    for i,e in enumerate(structure.entity_list):
        print(f"entity type            : {i} {e['type']}")
        print(f"entity description     : {i} {e['description']}")
        print(f"entity sequence        : {i} {e['sequence']}")
    print('\n')


def print_structure_data(structure):
    structure = _check_structure_or_tuple(structure)

    print("*** STRUCTURE DATA ***")
    print("Number of models : " + str(structure.num_models))
    print("Number of chains : " + str(structure.num_chains))
    print("Number of groups : " + str(structure.num_groups))
    print("Number of atoms : " + str(structure.num_atoms))
    print("Number of bonds : " + str(structure.num_bonds))
    print()

def print_chain_info(structure):
    structure = _check_structure_or_tuple(structure)

    print("*** CHAIN DATA ***")
    print("Number of chains: " + str(structure.num_chains))
    chainIndex = 0
    for i in range(0, structure.num_models):
        print("model: " + str(i+1))
        for j in range(0, structure.chains_per_model[i]):
            chainName = structure.chain_name_list[chainIndex]
            chainId = structure.chain_id_list[chainIndex]
            groups = structure.groups_per_chain[chainIndex]
            print("chainName: " + chainName + ", chainId: " + chainId + ", groups: " + str(groups))
            chainIndex = chainIndex + 1
    print()

def print_chain_group_info(structure):
    structure = _check_structure_or_tuple(structure)
    structure = structure.set_alt_loc_list()

    print("*** CHAIN AND GROUP DATA ***")
    chainIndex = 0
    groupIndex = 0
    for i in range(0, structure.num_models):
        print("model: " + str(i+1))
        for j in range(0, structure.chains_per_model[i]):
            chainName = structure.chain_name_list[chainIndex]
            chainId = structure.chain_id_list[chainIndex]
            groups = structure.groups_per_chain[chainIndex]
            print("chainName: " + chainName + ", chainId: " + chainId + ", groups: " + str(groups))
            for k in range(0, structure.groups_per_chain[chainIndex]):
                groupId = structure.group_id_list[groupIndex]
                insertionCode = structure.ins_code_list[groupIndex]
                secStruct = structure.sec_struct_list[groupIndex]
                seqIndex = structure.sequence_index_list[groupIndex]

                groupType = structure.group_type_list[groupIndex]

                groupName = structure.group_list[groupType]["groupName"]
                chemCompType = structure.group_list[groupType]["chemCompType"]
                oneLetterCode = structure.group_list[groupType]["singleLetterCode"]
                numAtoms = len(structure.group_list[groupType]["atomNameList"])
                numBonds = len(structure.group_list[groupType]["bondOrderList"])

                print("   groupName      : " + groupName)
                print("   oneLetterCode  : " + oneLetterCode)
                print("   seq. index     : " + str(seqIndex))
                print("   numAtoms       : " + str(numAtoms))
                print("   numBonds       : " + str(numBonds))
                print("   chemCompType   : " + chemCompType)
                print("   groupId        : " + str(groupId))
                print("   insertionCode  : " + insertionCode)
                print("   DSSP secStruct.: " + DsspSecondaryStructure.get_dssp_code(secStruct).get_one_letter_code())

                print()
                groupIndex = groupIndex + 1
            chainIndex = chainIndex + 1
    print()

def print_chain_entity_group_atom_info(structure):
    structure = _check_structure_or_tuple(structure)

    print("*** CHAIN ENTITY GROUP ATOM DATA ***")
    chainToEntityIndex = _get_chain_to_entity_index(structure)
    chainIndex = 0
    groupIndex = 0
    atomIndex = 0
    for i in range(0, structure.num_models):
        print("model: " + str(i+1))
        for j in range(0, structure.chains_per_model[i]):
            chainName = structure.chain_name_list[chainIndex]
            chainId = structure.chain_id_list[chainIndex]
            groups = structure.groups_per_chain[chainIndex]
            print("chainName: " + chainName + ", chainId: " + chainId + ", groups: " + str(groups))

            entityType = structure.entity_list[chainToEntityIndex[chainIndex]]["type"]
            entityDescription = structure.entity_list[chainToEntityIndex[chainIndex]]["description"]
            entitySequence = structure.entity_list[chainToEntityIndex[chainIndex]]["sequence"]
            print("entity type          : " + entityType);
            print("entity description   : " + entityDescription);
            print("entity sequence      : " + entitySequence);

            for k in range(0, structure.groups_per_chain[chainIndex]):
                groupId = structure.group_id_list[groupIndex]
                insertionCode = structure.ins_code_list[groupIndex]
                secStruct = structure.sec_struct_list[groupIndex]
                seqIndex = structure.sequence_index_list[groupIndex]

                groupType = structure.group_type_list[groupIndex]

                groupName = structure.group_list[groupType]["groupName"]
                chemCompType = structure.group_list[groupType]["chemCompType"]
                oneLetterCode = structure.group_list[groupType]["singleLetterCode"]
                numAtoms = len(structure.group_list[groupType]["atomNameList"])
                numBonds = len(structure.group_list[groupType]["bondOrderList"])

                print("   groupName      : " + groupName)
                print("   oneLetterCode  : " + oneLetterCode)
                print("   seq. index     : " + str(seqIndex))
                print("   numAtoms       : " + str(numAtoms))
                print("   numBonds       : " + str(numBonds))
                print("   chemCompType   : " + chemCompType)
                print("   groupId        : " + str(groupId))
                print("   insertionCode  : " + insertionCode)
                print("   DSSP secStruct.: " + DsspSecondaryStructure.get_dssp_code(secStruct).get_one_letter_code())
                print("   Atoms          : ")

                for m in range(0, (len(structure.group_list[groupType]["atomNameList"]))):
                    atomId = structure.atom_id_list[atomIndex]

                    if not structure.alt_loc_set:
                        structure = structure.set_alt_loc_list()

                    altLocId = structure.alt_loc_list[atomIndex]
                    x = structure.x_coord_list[atomIndex]
                    y = structure.y_coord_list[atomIndex]
                    z = structure.z_coord_list[atomIndex]
                    occupancy = structure.occupancy_list[atomIndex]
                    bFactor = structure.b_factor_list[atomIndex]

                    atomName = structure.group_list[groupType]["atomNameList"][m]
                    element = structure.group_list[groupType]["elementList"][m]

                    print("      " + str(atomId) + "\t" + atomName + "\t" + str(altLocId) +
                        "\t" + str(x) + "\t" + str(y) + "\t" + str(z) +
                        "\t" + str(occupancy) + "\t" + str(bFactor) + "\t" + str(element))
                    atomIndex = atomIndex + 1


                groupIndex = groupIndex + 1
            chainIndex = chainIndex + 1
    print('\n')

def print_all_data(structure):
    structure = _check_structure_or_tuple(structure)
    print_mmtf_info(structure)
    print_metadata(structure)
    print_crystallographic_data(structure)
    print_bioassembly_data(structure)
    print_structure_data(structure)
    print_chain_info(structure)
    print_chain_group_info(structure)
    print_chain_entity_group_atom_info(structure)
