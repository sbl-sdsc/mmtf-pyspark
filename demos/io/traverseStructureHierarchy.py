from pyspark import SparkConf, SparkContext
import sys
from mmtfPyspark.io.mmtfReader import download_mmtf_files, read_sequence_file
from mmtfPyspark.utils.DsspSecondaryStructure import *
import getopt
import sys


# Create variables
APP_NAME = "MMTF_Spark"


def main(argv):

    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    conf = conf.set("spark.executor.memory", "64g")
    conf = conf.set("spark.driver.cores", "32")
    sc = SparkContext(conf=conf)

    # Get command line input
    try:
        opts, args = getopt.getopt(argv, "p:", ["--path="])
    except getopt.GetoptError:
        print("traverse.py -p <path_to_mmtf>")
        sys.exit()
    for opt, arg in opts:
        if opt in ["-p", "--path"]:
            path = arg

    # Mmtf sequence file reader
    # pdbIds = ['1AQ1', '5GOD']
    # pdb = download_mmtf_files(pdbIds, sc)
    pdb = read_sequence_file(path, sc)

    def getChainToEntityIndex(structure):
        entityChainIndex = [0] * structure.num_chains
        for i in range(0, len(structure.entity_list)):
            for j in structure.entity_list[i]["chainIndexList"]:
                entityChainIndex[j] = i
        return entityChainIndex

    def listOfBytesToString(listOfBytes):
        newList = []
        for i in range(0, len(listOfBytes)):
            newList.append(listOfBytes[i].decode("utf-8"))
        return newList

    def listToString(temp):
        return("[" + ", ".join(map(str, temp)) + "]")

    def printMmtfInfo(structure):
        print("*** MMMTF INFO ***")
        print("MmtfProducer    : " + structure.mmtf_producer)
        print("MmtfVersion     : " + structure.mmtf_version)
        print()

    def printMetadata(structure):
        print("*** METADATA ***")
        print("StructureId           : " + structure.structure_id)
        print("Title                 : " + structure.title)
        print("Deposition date       : " + structure.deposition_date)
        print("Release date          : " + structure.release_date)
        print("Experimental method(s): " +
              listToString(listOfBytesToString(structure.experimental_methods)))
        print("Resolution            : " + str(structure.resolution))
        print("Rfree                 : " + str("%0.2f" % structure.r_free))
        print("Rwork                 : " + str("%0.2f" % structure.r_work))
        print()

    def printCrystallographicData(structure):
        print("*** CRYSTALLOGRAPHIC DATA ***")
        print("Space group           : " +
              structure.space_group.decode('utf-8'))
        print("Unit cell dimensions  : " +
              listToString(["%0.2f" % i for i in structure.unit_cell]))
        print()

    def printBioAssemblyData(structure):
        print("*** BIOASSEMBLY DATA ***")
        print("Number bioassemblies: " + str(len(structure.bio_assembly)))
        for i in range(0, len(structure.bio_assembly)):
            print("bioassembly: " +
                  structure.bio_assembly[i][b"name"].decode('utf-8'))
            transformations = structure.bio_assembly[i][b"transformList"]
            print("  Number transformations: " + str(len(transformations)))
            for j in range(0, len(transformations)):
                print("    transformation: " + str(j))
                print("    chains:         " +
                      str(transformations[j][b"chainIndexList"]))
                print("    rotTransMatrix: " +
                      str(transformations[j][b"matrix"]))

    def traverse(structure):
        print("*** STRUCTURE DATA ***")
        print("Number of models: " + str(structure.num_models))
        print("Number of chains: " + str(structure.num_chains))
        print("Number of groups: " + str(structure.num_groups))
        print("Number of atoms : " + str(structure.num_atoms))
        chainIndex = 0
        groupIndex = 0
        atomIndex = 0
        for i in range(0, structure.num_models):
            for j in range(0, structure.chains_per_model[i]):
                for k in range(0, structure.groups_per_chain[chainIndex]):
                    groupType = structure.group_type_list[groupIndex]
                    for m in range(0, (len(structure.group_list[groupType]["atomNameList"]))):
                        atomIndex = atomIndex + 1
                    groupIndex = groupIndex + 1
                chainIndex = chainIndex + 1
        print("chainIndex: " + str(chainIndex))
        print("groupIndex: " + str(groupIndex))
        print("atomIndex : " + str(atomIndex))
        print()

    def printChainInfo(structure):
        print("*** CHAIN DATA ***")
        print("Number of chains: " + str(structure.num_chains))
        chainIndex = 0
        for i in range(0, structure.num_models):
            print("model: " + str(i + 1))
            for j in range(0, structure.chains_per_model[i]):
                chainName = structure.chain_name_list[chainIndex]
                chainId = structure.chain_id_list[chainIndex]
                groups = structure.groups_per_chain[chainIndex]
                print("chainName: " + chainName + ", chainId: " +
                      chainId + ", groups: " + str(groups))
                chainIndex = chainIndex + 1
        print()

    def printChainGroupInfo(structure):
        print("*** CHAIN AND GROUP DATA ***")
        chainIndex = 0
        groupIndex = 0
        for i in range(0, structure.num_models):
            print("model: " + str(i + 1))
            for j in range(0, structure.chains_per_model[i]):
                chainName = structure.chain_name_list[chainIndex]
                chainId = structure.chain_id_list[chainIndex]
                groups = structure.groups_per_chain[chainIndex]
                print("chainName: " + chainName + ", chainId: " +
                      chainId + ", groups: " + str(groups))
                for k in range(0, structure.groups_per_chain[chainIndex]):
                    groupId = structure.group_id_list[groupIndex]
                    insertionCode = structure.ins_code_list[groupIndex]
                    secStruct = structure.sec_struct_list[groupIndex]
                    seqIndex = structure.sequence_index_list[groupIndex]

                    groupType = structure.group_type_list[groupIndex]

                    groupName = structure.group_list[groupType]["groupName"]
                    chemCompType = structure.group_list[groupType]["chemCompType"]
                    oneLetterCode = structure.group_list[groupType]["singleLetterCode"]
                    numAtoms = len(
                        structure.group_list[groupType]["atomNameList"])
                    numBonds = len(
                        structure.group_list[groupType]["bondOrderList"])

                    print("   groupName      : " + groupName)
                    print("   oneLetterCode  : " + oneLetterCode)
                    print("   seq. index     : " + str(seqIndex))
                    print("   numAtoms       : " + str(numAtoms))
                    print("   numBonds       : " + str(numBonds))
                    print("   chemCompType   : " + chemCompType)
                    print("   groupId        : " + str(groupId))
                    print("   insertionCode  : " + insertionCode)
                    print("   DSSP secStruct.: " +
                          DsspSecondaryStructure.get_dssp_code(secStruct).get_one_letter_code())
                    print()
                    groupIndex = groupIndex + 1
                chainIndex = chainIndex + 1
        print()

    def printChainEntityGroupAtomInfo(structure):
        print("*** CHAIN ENTITY GROUP ATOM DATA ***")
        chainToEntityIndex = getChainToEntityIndex(structure)
        chainIndex = 0
        groupIndex = 0
        atomIndex = 0
        for i in range(0, structure.num_models):
            print("model: " + str(i + 1))
            for j in range(0, structure.chains_per_model[i]):
                chainName = structure.chain_name_list[chainIndex]
                chainId = structure.chain_id_list[chainIndex]
                groups = structure.groups_per_chain[chainIndex]
                print("chainName: " + chainName + ", chainId: " +
                      chainId + ", groups: " + str(groups))

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
                    numAtoms = len(
                        structure.group_list[groupType]["atomNameList"])
                    numBonds = len(
                        structure.group_list[groupType]["bondOrderList"])

                    print("   groupName      : " + groupName)
                    print("   oneLetterCode  : " + oneLetterCode)
                    print("   seq. index     : " + str(seqIndex))
                    print("   numAtoms       : " + str(numAtoms))
                    print("   numBonds       : " + str(numBonds))
                    print("   chemCompType   : " + chemCompType)
                    print("   groupId        : " + str(groupId))
                    print("   insertionCode  : " + insertionCode)
                    print("   DSSP secStruct.: " +
                          DsspSecondaryStructure.get_dssp_code(secStruct).get_one_letter_code())
                    print("   Atoms          : ")

                    for m in range(0, (len(structure.group_list[groupType]["atomNameList"]))):
                        atomId = structure.atom_id_list[atomIndex]
                        altLocId = structure.alt_loc_list[atomIndex]
                        x = structure.x_coord_list[atomIndex]
                        y = structure.y_coord_list[atomIndex]
                        z = structure.z_coord_list[atomIndex]
                        occupancy = structure.occupancy_list[atomIndex]
                        bFactor = structure.b_factor_list[atomIndex]

                        atomName = structure.group_list[groupType]["atomNameList"][m]
                        element = structure.group_list[groupType]["elementList"][m]

                        print("      " + str(atomId) + "\t" + atomName + "\t" + altLocId +
                              "\t" + str(x) + "\t" + str(y) + "\t" + str(z) +
                              "\t" + str(occupancy) + "\t" + str(bFactor) + "\t" + element)
                        atomIndex = atomIndex + 1

                    groupIndex = groupIndex + 1
                chainIndex = chainIndex + 1
        print()

    def TraverseStructureHierarchy(structure):
        structure = structure.set_alt_loc_list()
        print(structure.entity_list)
        printMmtfInfo(structure)
        printMetadata(structure)
        printCrystallographicData(structure)
        traverse(structure)
        printChainInfo(structure)
        printChainGroupInfo(structure)
        printChainEntityGroupAtomInfo(structure)
        printBioAssemblyData(structure)

    pdb.foreach(lambda t: TraverseStructureHierarchy(t[1]))


if __name__ == "__main__":
    # Execute Main functionality
    main(sys.argv[1:])
