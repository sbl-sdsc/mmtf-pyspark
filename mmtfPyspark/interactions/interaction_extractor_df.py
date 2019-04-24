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


class InteractionExtractorDf(object):

    @staticmethod
    def get_interactions(structure, query, target, distance_cutoff, inter=True, intra=False, level='group'):
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
        query : Pandas query string to select 'query' atoms
        target: Pandas query string to select 'target' atoms
        level : 'chain', 'group' or 'atom' to aggregate results

        Returns
        -------
        dataset
           dataset with interacting residue and atom information
        '''

        # find all interactions
        row = structure.flatMap(InteractionFingerprint(query, target, distance_cutoff, inter, intra, level))

        # TODO consider adding parameters
        # chem: add element, entity_type(LGO, PRO, DNA, etc.)
        # geom=True -> add distance, order parameters([q3,q4,q5,q6]
        # seq=True -> add sequence index, sequence

        # Convert RDD of rows to a dataset using a schema
        spark = SparkSession.builder.getOrCreate()
        schema = InteractionExtractorDf._get_schema(level)
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
                      # StructField("sequenceIndex", IntegerType(), nullable),
                      # StructField("sequence", StringType(), nullable)
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
                      # StructField("sequenceIndex", IntegerType(), nullable),
                      # StructField("sequence", StringType(), nullable)
                      ]

        schema = StructType(fields)
        return schema


class InteractionFingerprint:

    def __init__(self, query, target, distance_cutoff, inter, intra, level='group'):
        self.query = query
        self.target = target
        self.distance_cutoff = distance_cutoff
        self.inter = inter
        self.intra = intra
        self.level = level

    def __call__(self, t):
        structure_id = t[0]
        structure = t[1]

        # if there is only a single chain, there are no intermolecular interactions
        if structure.num_chains == 1 and self.inter and not self.intra:
            return []

        df = ColumnarStructure(structure, True).get_df()
        if df is None:
            return []

        # Apply query filter
        try:
            q = df.query(self.query)
        except:
            return []

        if q is None or q.shape[0] == 0:
            return []

        # Apply target filter
        if self.target == self.query:
            t = q
        else:
            try:
                t = df.query(self.target)
            except:
                return []

        if t is None or t.shape[0] == 0:
            return []

        # Stack coordinates into an nx3 array
        cq = np.column_stack((q['x'].values, q['y'].values, q['z'].values))
        ct = np.column_stack((t['x'].values, t['y'].values, t['z'].values))

        # Calculate distances between the two atom sets
        tree_t = cKDTree(ct)
        tree_q = cKDTree(cq)
        sparse_dm = tree_t.sparse_distance_matrix(tree_q, max_distance=self.distance_cutoff, output_type='dict')

        # Add interactions to rows.
        # There are redundant interactions when aggregating the results at the 'group' level,
        # since multiple atoms in a group may be involved in interactions.
        # Therefore we use a set of rows to store only unique interactions.
        rows = set([])
        for ind, dis in sparse_dm.items():
            i = ind[0]  # polymer target atom index
            j = ind[1]  # polymer query atom index
            print(structure_id, i, j, dis)

            tr = t.iloc[[i]]
            qr = q.iloc[[j]]

            # handle intra vs inter-chain interactions
            if qr['chain_name'].item() == tr['chain_name'].item():
                # cases with interactions in the same chain
                if not self.intra:
                    # exclude intrachain interactions
                    continue

                elif qr['group_name'].item() == tr['group_name'].item():
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
                row = Row(structure_id + "." + tr['chain_name'].item(),  # structureChainId
                          qr['group_name'].item(),  # queryGroupId
                          qr['chain_name'].item(),  # queryChainId
                          qr['group_number'].item(),  # queryGroupNumber
                          tr['chain_name'].item()  # targetChainId
                          )
                rows.add(row)

            elif self.level == 'group':
                row = Row(structure_id + "." + tr['chain_name'].item(),  # structureChainId
                          qr['group_name'].item(),  # queryGroupId
                          qr['chain_name'].item(),  # queryChainId
                          qr['group_number'].item(),  # queryGroupNumber
                          tr['group_name'].item(),  # targetGroupId
                          tr['chain_name'].item(),  # targetChainId
                          tr['group_number'].item(),  # targetGroupNumber
                          )
                rows.add(row)

            elif self.level == 'atom':
                row = Row(structure_id + "." + tr['chain_name'].item(),  # structureChainId
                          qr['group_name'].item(),  # queryGroupId
                          qr['chain_name'].item(),  # queryChainId
                          qr['group_number'].item(),  # queryGroupNumber
                          qr['atom_name'].item(),  # queryAtomName
                          tr['group_name'].item(),  # targetGroupId
                          tr['chain_name'].item(),  # targetChainId
                          tr['group_number'].item(),  # targetGroupNumber
                          tr['atom_name'].item(),  # targetAtomName
                          dis,  # distance
                          )
                rows.add(row)

        return rows

