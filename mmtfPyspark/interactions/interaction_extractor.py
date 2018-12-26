#!/user/bin/env python
'''interaction_extractor.py

This class creates dataset of ligand - macromolecule and macromolecule -
macromolecule interaction information. Criteria to select interactions are
specified by the InteractionFilter.

'''
__author__ = "Peter W Rose"
__version__ = "0.3.0"
__status__ = "experimental"

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from mmtfPyspark.utils import ColumnarStructure
from pyspark.sql import Row
import numpy as np
from scipy.spatial import cKDTree


class InteractionExtractor(object):

    @staticmethod
    def get_ligand_polymer_interactions(structures, interaction_filter, level='group'):
        '''Returns a dataset of ligand - macromolecule interactions

        The dataset contains the following columns:
        - structureChainId - pdbId.chainName of chain that interacts with ligand
        - queryLigandId - id of ligand from PDB chemical component dictionary
        - queryLigandNumber - group number of ligand including insetion code
        - queryLigandChainId - chain name of ligand
        - targetChainId - name of chain for which the interaction data are listed
        - groupNumbers - array of residue number of interacting groups including insertion code (e.g. 101A)
        - sequenceIndices - array of zero-based index of interaction groups (residues) mapped onto target sequence
        - sequence - interacting polymer sequence
        - interactingChains - total number of chains that interact with ligand

        Parameters
        ----------
        structures : PythonRDD
           a set of PDB structures
        interaction_filter : InteractionFilter
           interaction criteria
        level : 'group' or 'atom' to aggregate results

        Returns
        -------
        dataset
           dataset with interacting residue information
        '''

        # find sll interactions
        row = structures.flatMap(LigandInteractionFingerprint(interaction_filter, level))

        # Convert RDD to a Dataset with the following columns and types
        nullable = False

        # consider adding parameters
        # chem: add element, entity_type(LGO, PRO, DNA, etc.)
        # geom=True -> add distance, order parameters([q3,q4,q5,q6]
        # seq=True -> add sequence index, sequence

        fields = []
        if level == 'group':
            fields = [StructField("structureChainId", StringType(), nullable),
                      StructField("queryLigandId", StringType(), nullable),
                      StructField("queryLigandChainId", StringType(), nullable),
                      StructField("queryLigandNumber", StringType(), nullable),
                      StructField("targetGroupId", StringType(), nullable),
                      StructField("targetChainId", StringType(), nullable),
                      StructField("targetGroupNumber", StringType(), nullable),
                      StructField("sequenceIndex", IntegerType(), nullable),
                      StructField("sequence", StringType(), nullable)
                      ]
        elif level == 'atom':
            fields = [StructField("structureChainId", StringType(), nullable),
                      StructField("queryLigandId", StringType(), nullable),
                      StructField("queryLigandChainId", StringType(), nullable),
                      StructField("queryLigandNumber", StringType(), nullable),
                      StructField("queryAtomName", StringType(), nullable),
                      StructField("targetChainId", StringType(), nullable),
                      StructField("targetGroupId", StringType(), nullable),
                      StructField("targetGroupNumber", StringType(), nullable),
                      StructField("targetAtomName", StringType(), nullable),
                      StructField("distance", FloatType(), nullable),
                      StructField("sequenceIndex", IntegerType(), nullable),
                      StructField("sequence", StringType(), nullable)
                      ]

        schema = StructType(fields)
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(row, schema)


class LigandInteractionFingerprint:

    def __init__(self, interaction_filter, level='group'):
        self.filter = interaction_filter
        self.level = level

    def __call__(self, t):
        structure_id = t[0]
        structure = t[1]

        arrays = ColumnarStructure(structure, True)

        # Apply query (ligand) filter
        group_names = arrays.get_group_names()
        qg = self.filter.is_query_group_np(group_names)
        if np.count_nonzero(qg) == 0:
            return []

        elements = arrays.get_elements()
        qe = self.filter.is_query_element_np(elements)
        if np.count_nonzero(qe) == 0:
            return []

        atom_names = arrays.get_atom_names()
        qa = self.filter.is_query_atom_name_np(atom_names)
        if np.count_nonzero(qa) == 0:
            return []

        ### filter prohibited groups??

        # Create mask for polymer atoms
        polymer = arrays.is_polymer()

        # Create mask for ligand atoms
        lig = ~polymer & qg & qe & qa
        if np.count_nonzero(lig) == 0:
            return []

        # Apply target (polymer) filter
        tg = self.filter.is_target_group_np(group_names)
        te = self.filter.is_target_element_np(elements)
        ta = self.filter.is_target_atom_name_np(atom_names)

        poly = polymer & tg & te & ta

        if np.count_nonzero(poly) == 0:
            return []

        chain_names = arrays.get_chain_names()
        group_numbers = arrays.get_group_numbers()
        entity_indices = arrays.get_entity_indices()
        sequence_positions = arrays.get_sequence_positions()

        # Stack coordinates into an nx3 array
        # TODO add this to ColumnarStructure
        c = np.stack((arrays.get_x_coords(), arrays.get_y_coords(), arrays.get_z_coords()), axis=-1)

        # Apply ligand mask to ligand data
        c_ligand = c[lig]
        lg = group_names[lig]
        ln = group_numbers[lig]
        la = atom_names[lig]
        lc = chain_names[lig]

        # Apply polymer mask to polymer data
        c_polymer = c[poly]
        pg = group_names[poly]
        pn = group_numbers[poly]
        pa = atom_names[poly]
        pc = chain_names[poly]
        pt = entity_indices[poly]
        ps = sequence_positions[poly]

        # Calculate distances between polymer and ligand atoms
        poly_tree = cKDTree(c_polymer)
        lig_tree = cKDTree(c_ligand)
        distance_cutoff = self.filter.get_distance_cutoff()
        sparse_dm = poly_tree.sparse_distance_matrix(lig_tree, max_distance=distance_cutoff, output_type='dict')

        # Add interactions to rows.
        # There are redundant interactions when aggregating the results at the 'group' level,
        # since multiple atoms in a group may be involved in interactions.
        # Therefore we use a set of rows to store only unique interactions.
        rows = set([])
        for ind, dis in sparse_dm.items():
            i = ind[0]  # ligand atom index
            j = ind[1]  # polymer atom index
            if self.level == 'group':
                row = Row(structure_id + "." + pc[i],  # structureChainId
                          lg[j],  # queryLigandId
                          lc[j],  # queryLigandChainId
                          ln[j],  # queryLigandNumber
                          pg[i],  # targetGroupId
                          pc[i],  # targetChainId
                          pn[i],  # targetGroupNumber
                          ps[i].item(),  # sequenceIndex
                          structure.entity_list[pt[i]]['sequence']  # sequence
                          )
            elif self.level == 'atom':
                row = Row(structure_id + "." + pc[i],  # structureChainId
                          lg[j],  # queryLigandId
                          lc[j],  # queryLigandChainId
                          ln[j],  # queryLigandNumber
                          la[j],  # queryAtomName
                          pg[i],  # targetGroupId
                          pc[i],  # targetChainId
                          pn[i],  # targetGroupNumber
                          pa[i],  # targetAtomName
                          dis,  # distance
                          ps[i].item(),  # sequenceIndex
                          structure.entity_list[pt[i]]['sequence']  # sequence
                          )

            rows.add(row)

        return rows

#class PolymerInteractionFingerprint(object):
    #  TODO create a KD tree for each chain, then do comparisons?

#class GroupInteractionFingerprint(object):
    # TODO calculate interaction of a group with both polymers and ligands
