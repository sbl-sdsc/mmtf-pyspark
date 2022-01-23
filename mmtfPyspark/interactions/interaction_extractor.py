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

        The dataset contains the following columns. When level='chain' or level='group' is specified,
        only a subset of these columns is returned.
        - structureChainId - pdbId.chainName of interacting chain
        - queryGroupId - id of the query group (residue) from the PDB chemical component dictionary
        - queryChainId - chain name of the query group (residue)
        - queryGroupNumber - group number of the query group (residue) including insertion code (e.g. 101A)
        - queryAtomName - atom name of the query atom
        - targetGroupId - id of the query group (residue) from the PDB chemical component dictionary
        - targetChainId - chain name of the target group (residue)
        - targetGroupNumber - group number of the target group (residue) including insertion code (e.g. 101A)
        - targetAtomName - atom name of the target atom
        - distance - distance between interaction atoms
        - sequenceIndex - zero-based index of interacting groups (residues) mapped onto target sequence
        - sequence - interacting polymer sequence

        Parameters
        ----------
        structures : PythonRDD
           a set of PDB structures
        interaction_filter : InteractionFilter
           interaction criteria
        level : 'chain', 'group' or 'atom' to aggregate results

        Returns
        -------
        dataset
           dataset with interacting residue and atom information
        '''

        # find all interactions
        row = structures.flatMap(LigandInteractionFingerprint(interaction_filter, level))

        # TODO consider adding parameters
        # chem: add element, entity_type(LGO, PRO, DNA, etc.)
        # geom=True -> add distance, order parameters([q3,q4,q5,q6]
        # seq=True -> add sequence index, sequence

        # Convert RDD of rows to a dataset using a schema
        spark = SparkSession.builder.getOrCreate()
        schema = InteractionExtractor._get_schema(level)
        return spark.createDataFrame(row, schema)


    @staticmethod
    def get_polymer_interactions(structures, interaction_filter, inter=True, intra=False, level='group'):
        '''Returns a dataset of inter and or intra macromolecule - macromolecule interactions

        The dataset contains the following columns. When level='chain' or level='group' is specified,
        only a subset of these columns is returned.
        - structureChainId - pdbId.chainName of interacting chain
        - queryGroupId - id of the query group (residue) from the PDB chemical component dictionary
        - queryChainId - chain name of the query group (residue)
        - queryGroupNumber - group number of the query group (residue) including insertion code (e.g. 101A)
        - queryAtomName - atom name of the query atom
        - targetGroupId - id of the query group (residue) from the PDB chemical component dictionary
        - targetChainId - chain name of the target group (residue)
        - targetGroupNumber - group number of the target group (residue) including insertion code (e.g. 101A)
        - targetAtomName - atom name of the target atom
        - distance - distance between interaction atoms
        - sequenceIndex - zero-based index of interacting groups (residues) mapped onto target sequence
        - sequence - interacting polymer sequence

        Parameters
        ----------
        structures : PythonRDD
           a set of PDB structures
        interaction_filter : InteractionFilter
           interaction criteria
        inter : calculate inter-chain interactions (default True)
        intra : calculate intra-chain interactions (default False)
        level : 'chain', 'group' or 'atom' to aggregate results

        Returns
        -------
        dataset
           dataset with interacting residue and atom information
        '''

        # find all interactions
        row = structures.flatMap(PolymerInteractionFingerprint(interaction_filter, inter, intra, level))

        # Convert RDD of rows to a dataset using a schema
        spark = SparkSession.builder.getOrCreate()
        schema = InteractionExtractor._get_schema(level)
        return spark.createDataFrame(row, schema)

    @staticmethod
    def _get_schema(level):
        fields = []
        nullable = False

        if level == 'chain':
            fields = [StructField("structureChainId", StringType(), nullable),
                      StructField("queryGroupId", StringType(), nullable),
                      StructField("queryChainId", StringType(), nullable),
                      StructField("queryGroupNumber", StringType(), nullable),
                      StructField("targetChainId", StringType(), nullable)
                      ]
        elif level == 'group':
            fields = [StructField("structureChainId", StringType(), nullable),
                      StructField("queryGroupId", StringType(), nullable),
                      StructField("queryChainId", StringType(), nullable),
                      StructField("queryGroupNumber", StringType(), nullable),
                      StructField("targetGroupId", StringType(), nullable),
                      StructField("targetChainId", StringType(), nullable),
                      StructField("targetGroupNumber", StringType(), nullable),
                      StructField("sequenceIndex", IntegerType(), nullable),
                      StructField("sequence", StringType(), nullable)
                      ]
        elif level == 'atom':
            fields = [StructField("structureChainId", StringType(), nullable),
                      StructField("queryGroupId", StringType(), nullable),
                      StructField("queryChainId", StringType(), nullable),
                      StructField("queryGroupNumber", StringType(), nullable),
                      StructField("queryAtomName", StringType(), nullable),
                      StructField("targetGroupId", StringType(), nullable),
                      StructField("targetChainId", StringType(), nullable),
                      StructField("targetGroupNumber", StringType(), nullable),
                      StructField("targetAtomName", StringType(), nullable),
                      StructField("distance", FloatType(), nullable),
                      StructField("sequenceIndex", IntegerType(), nullable),
                      StructField("sequence", StringType(), nullable)
                      ]

        schema = StructType(fields)
        return schema


class LigandInteractionFingerprint:

    def __init__(self, interaction_filter, level='group'):
        self.filter = interaction_filter
        self.level = level

    def __call__(self, t):
        structure_id = t[0]
        structure = t[1]

        #print(structure_id)
        #arrays = ColumnarStructure(structure, True)

        # Apply query (ligand) filter
        #group_names = arrays.get_group_names()
        group_names = structure.group_names
        #print("group_names:", group_names.shape, group_names.dtype.name)
        qg = self.filter.is_query_group_np(group_names)
        if np.count_nonzero(qg) == 0:
            return []
        #print("qg:", qg.shape, qg.dtype.name)

        #elements = arrays.get_elements()
        elements = structure.elements
        qe = self.filter.is_query_element_np(elements)
        if np.count_nonzero(qe) == 0:
            return []

        #atom_names = arrays.get_atom_names()
        atom_names = structure.atom_names
        qa = self.filter.is_query_atom_name_np(atom_names)
        if np.count_nonzero(qa) == 0:
            return []

        ### filter prohibited groups??

        # Create mask for polymer atoms
        #polymer = arrays.is_polymer()
        polymer = structure.polymer

        # Create mask for ligand atoms
        lig = ~polymer & qg & qe & qa
        if np.count_nonzero(lig) == 0:
            return []

        #print("lig:", lig.shape, lig.dtype.name)

        # Apply target (polymer) filter
        tg = self.filter.is_target_group_np(group_names)
        te = self.filter.is_target_element_np(elements)
        ta = self.filter.is_target_atom_name_np(atom_names)

        poly = polymer & tg & te & ta

        if np.count_nonzero(poly) == 0:
            return []

        #chain_names = arrays.get_chain_names()
        #group_numbers = arrays.get_group_numbers()
        #entity_indices = arrays.get_entity_indices()
        #sequence_positions = arrays.get_sequence_positions()
        chain_names = structure.chain_names
        group_numbers = structure.group_numbers
        entity_indices = structure.entity_indices
        sequence_positions = structure.sequence_positions

        # Stack coordinates into an nx3 array
        # TODO add this to ColumnarStructure
        #c = np.stack((arrays.get_x_coords(), arrays.get_y_coords(), arrays.get_z_coords()), axis=-1)
        c = np.stack((structure.x_coord_list, structure.y_coord_list, structure.z_coord_list), axis=-1)
        # Apply ligand mask to ligand data
        c_ligand = c[lig]

        #print("group numbers:", group_numbers.shape[0], group_numbers.tolist())
        #print("lig flags:", lig.shape[0], lig.tolist())
        lg = group_names[lig]
        #print("ligand group numbers", lg.tolist())
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
            if self.level == 'chain':
                row = Row(structure_id + "." + pc[i],  # structureChainId
                          lg[j],  # queryLigandId
                          lc[j],  # queryLigandChainId
                          ln[j],  # queryLigandNumber
                          pc[i]  # targetChainId
                          )
                rows.add(row)
            elif self.level == 'group':
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
                rows.add(row)
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

class PolymerInteractionFingerprint:

    def __init__(self, interaction_filter, inter, intra, level='group'):
        self.filter = interaction_filter
        self.inter = inter
        self.intra = intra
        self.level = level

    def __call__(self, t):
        structure_id = t[0]
        structure = t[1]

        arrays = ColumnarStructure(structure, True)

        # if there is only a single chain, there are no intermolecular interactions
        if structure.num_chains == 1 and self.inter and not self.intra:
            return []

        # Apply query filter
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

        # Create mask for polymer atoms
        polymer = arrays.is_polymer()

        # Apply query filter to polymer
        polyq = polymer & qg & qe & qa

        if np.count_nonzero(polyq) == 0:
            return []

        # Apply target filter to polymer atoms
        tg = self.filter.is_target_group_np(group_names)
        te = self.filter.is_target_element_np(elements)
        ta = self.filter.is_target_atom_name_np(atom_names)

        polyt = polymer & tg & te & ta

        if np.count_nonzero(polyt) == 0:
            return []

        chain_names = arrays.get_chain_names()
        group_numbers = arrays.get_group_numbers()
        entity_indices = arrays.get_entity_indices()
        sequence_positions = arrays.get_sequence_positions()

        # Stack coordinates into an nx3 array
        # TODO add this to ColumnarStructure
        c = np.stack((arrays.get_x_coords(), arrays.get_y_coords(), arrays.get_z_coords()), axis=-1)

        # Apply mask for query atoms
        cpq = c[polyq]
        pgq = group_names[polyq]
        pnq = group_numbers[polyq]
        paq = atom_names[polyq]
        pcq = chain_names[polyq]

        # Apply mask for target atoms
        cpt = c[polyt]
        pgt = group_names[polyt]
        pnt = group_numbers[polyt]
        pat = atom_names[polyt]
        pct = chain_names[polyt]
        pet = entity_indices[polyt]
        pst = sequence_positions[polyt]

        # Calculate distances between the two atom sets
        tree_t = cKDTree(cpt)
        tree_q = cKDTree(cpq)
        distance_cutoff = self.filter.get_distance_cutoff()
        sparse_dm = tree_t.sparse_distance_matrix(tree_q, max_distance=distance_cutoff, output_type='dict')

        # Add interactions to rows.
        # There are redundant interactions when aggregating the results at the 'group' level,
        # since multiple atoms in a group may be involved in interactions.
        # Therefore we use a set of rows to store only unique interactions.
        rows = set([])
        for ind, dis in sparse_dm.items():
            i = ind[0]  # polymer target atom index
            j = ind[1]  # polymer query atom index

            # handle intra vs inter-chain interactions
            if pcq[j] == pct[i]:
                # cases with interactions in the same chain
                if not self.intra:
                    # exclude intrachain interactions
                    continue

                elif pnq[j] == pnt[i]:
                    # exclude interactions within the same chain and group
                    continue

            else:
                # case with interactions in different chains
                if not self.inter:
                    # exclude inter-chain interactions
                    continue

            # exclude self interactions (this can happen if the query and target criteria overlap)
            if dis < 0.001:
                continue

            if self.level == 'chain':
                row = Row(structure_id + "." + pct[i],  # structureChainId
                          pgq[j],  # queryGroupId
                          pcq[j],  # queryChainId
                          pnq[j],  # queryGroupNumber
                          pct[i]  # targetChainId
                          )
                rows.add(row)
            elif self.level == 'group':
                row = Row(structure_id + "." + pct[i],  # structureChainId
                          pgq[j],  # queryGroupId
                          pcq[j],  # queryChainId
                          pnq[j],  # queryGroupNumber
                          pgt[i],  # targetGroupId
                          pct[i],  # targetChainId
                          pnt[i],  # targetGroupNumber
                          pst[i].item(),  # sequenceIndex
                          structure.entity_list[pet[i]]['sequence']  # sequence
                          )
                rows.add(row)
            elif self.level == 'atom':
                row = Row(structure_id + "." + pct[i],  # structureChainId
                          pgq[j],  # queryGroupId
                          pcq[j],  # queryChainId
                          pnq[j],  # queryGroupNumber
                          paq[j],  # queryAtomName
                          pgt[i],  # targetGroupId
                          pct[i],  # targetChainId
                          pnt[i],  # targetGroupNumber
                          pat[i],  # targetAtomName
                          dis,  # distance
                          pst[i].item(),  # sequenceIndex
                          structure.entity_list[pet[i]]['sequence']  # sequence
                          )
                rows.add(row)

        return rows

