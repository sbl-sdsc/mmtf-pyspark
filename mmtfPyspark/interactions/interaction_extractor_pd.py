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
#from pyspark.sql import Row
import numpy as np
from scipy.spatial import cKDTree


class InteractionExtractorPd(object):

    @staticmethod
    def get_interactions(structure, query, target, distance_cutoff, inter=True, intra=False, bio=None, level='group'):
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
        if bio is None:
            row = structure.flatMap(InteractionFingerprint(query, target, distance_cutoff, inter, intra, level))
        else:
            row = structure.flatMap(BioInteractionFingerprint(query, target, distance_cutoff, inter, intra, bio, level))

        # TODO consider adding parameters
        # chem: add element, entity_type(LGO, PRO, DNA, etc.)
        # geom=True -> add distance, order parameters([q3,q4,q5,q6]
        # seq=True -> add sequence index, sequence

        # Convert RDD of rows to a dataset using a schema
        spark = SparkSession.builder.getOrCreate()
        #schema = InteractionExtractorDf._get_schema(level)
        schema = InteractionExtractorPd._get_schema_new(level, bio)
        return spark.createDataFrame(row, schema)

    @staticmethod
    def _get_schema_new(level, bio=0):
        fields = []
        nullable = False

        fields.append(StructField("structure_chain_id", StringType(), nullable))

        # define query columns
        fields.append(StructField("q_chain_name", StringType(), nullable))
        if bio is not None:
            fields.append(StructField("q_trans", IntegerType(), nullable))
        fields.append(StructField("q_group_name", StringType(), nullable))
        fields.append(StructField("q_group_number", StringType(), nullable))
        if level == 'atom':
            fields.append(StructField("q_atom_name", StringType(), nullable))

        # define target columns
        fields.append(StructField("t_chain_name", StringType(), nullable))
        if bio is not None:
            fields.append(StructField("t_trans", IntegerType(), nullable))
        if level == 'group' or level == 'atom' or level == 'coord':
            fields.append(StructField("t_group_name", StringType(), nullable))
            fields.append(StructField("t_group_number", StringType(), nullable))
        if level == 'atom' or level == 'coord':
            fields.append(StructField("t_atom_name", StringType(), nullable))
            fields.append(StructField("distance", FloatType(), nullable))
                      # StructField("sequenceIndex", IntegerType(), nullable),
                      # StructField("sequence", StringType(), nullable)

        if level == 'coord':
            fields.append(StructField("q_x", FloatType(), nullable))
            fields.append(StructField("q_y", FloatType(), nullable))
            fields.append(StructField("q_z", FloatType(), nullable))
            fields.append(StructField("t_x", FloatType(), nullable))
            fields.append(StructField("t_y", FloatType(), nullable))
            fields.append(StructField("t_z", FloatType(), nullable))

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

        # Get a dataframe representation of the structure
        df = ColumnarStructure(structure, True).to_pandas()
        if df is None:
            return []

        # Apply query filter
        q = df.query(self.query)

        if q is None or q.shape[0] == 0:
            return []

        # Apply target filter
        if self.target == self.query:
            # if query and target are identical, reuse the query dataframe
            t = q
        else:
            t = df.query(self.target)

        if t is None or t.shape[0] == 0:
            return []

        # Stack coordinates into an nx3 array
        cq = np.column_stack((q['x'].values, q['y'].values, q['z'].values))
        ct = np.column_stack((t['x'].values, t['y'].values, t['z'].values))

        # Calculate distances between the two atom sets
#        tree_t = cKDTree(ct)
#        tree_q = cKDTree(cq)

        # return calc_interactions(structure_id, q, t, tree_q, tree_t, self.inter, self.intra,
        #                              self.level, self.distance_cutoff)

        return calc_interactions(structure_id, q, t, cq, ct, self.inter, self.intra,
                             self.level, self.distance_cutoff)


class BioInteractionFingerprint:

    def __init__(self, query, target, distance_cutoff, inter, intra, bio=0, level='group'):
        self.query = query
        self.target = target
        self.distance_cutoff = distance_cutoff
        self.inter = inter
        self.intra = intra
        self.bio = bio
        self.level = level

    def __call__(self, t):
        structure_id = t[0]

        # if the specified bio assembly does not exist, return an empty list
        if len(t[1].bio_assembly) < self.bio:
            return []

        structure = ColumnarStructure(t[1], True)

        # Get a dataframe representation of the structure
        df = structure.get_df()
        if df is None:
            return []

        # Apply query filter
        q = df.query(self.query)

        if q is None or q.shape[0] == 0:
            return []

        # Apply target filter
        if self.target == self.query:
            # if query and target are identical, reuse the query dataframe
            t = q
        else:
            t = df.query(self.target)

        if t is None or t.shape[0] == 0:
            return []

        # group by chain ids and convert grouped df to a regular df
        q_chains = q.groupby('chain_id')
        t_chains = t.groupby('chain_id')

        rows = list()

        # Find interactions between pairs of chains in bio assembly
        transforms = self.get_transforms(structure)
        for q_transform in transforms:
            qindex = q_transform[0]  # transformation id
            qchain = q_transform[1]  # chain id

            if qchain in q_chains.groups.keys():
                qt = q_chains.get_group(qchain).reset_index(drop=True)
            else:
                continue

            qmat = np.array(q_transform[2]).reshape((4, 4))  # matrix

            for t_transform in transforms:
                tindex = t_transform[0]
                tchain = t_transform[1]

                # exclude intra interactions (same transformation and same chain id)
                if not self.intra and qindex == tindex and qchain == tchain:
                    continue

                if tchain in t_chains.groups.keys():
                    tt = t_chains.get_group(tchain).reset_index(drop=True)
                else:
                    continue

                print("q:", qindex, qchain, "t:", tindex, tchain)
                print("qt:", qt.shape[0], " tt:", tt.shape[0])

                # reshape to a 4x4 transformation matrix
                tmat = np.array(t_transform[2]).reshape((4, 4))

                # Stack coordinates into an nx3 array
                cq = np.column_stack((qt['x'].values, qt['y'].values, qt['z'].values)).copy()
                ct = np.column_stack((tt['x'].values, tt['y'].values, tt['z'].values)).copy()

                # Apply bio assembly transformations
                # apply rotation
                cqt = np.matmul(cq, qmat[0:3, 0:3])
                # apply translation
                cqt += qmat[3, 0:3].transpose()
                # apply rotation
                ctt = np.matmul(ct, tmat[0:3, 0:3])
                # apply translation
                ctt += tmat[3, 0:3].transpose()

                rows += calc_interactions(structure_id, qt, tt, cqt, ctt, self.inter, self.intra,
                                          self.level, self.distance_cutoff, self.bio, qindex, tindex)

        return rows

    def get_transforms(self, col):
        """Return a dictionary of transformation index, chain indices/transformation matrices for given bio assembly"""
        trans = list()
        chain_ids = col.structure.chain_id_list
        assembly = col.structure.bio_assembly[self.bio - 1]  # bio assembly id are one-based
        for id, transforms in enumerate(assembly['transformList']):
            for index in transforms['chainIndexList']:
                trans.append((id, chain_ids[index], transforms['matrix']))
        return trans


#def calc_interactions(structure_id, q, t, tree_q, tree_t, inter, intra, level, distance_cutoff, bio=None, qindex=0, tindex=0):
def calc_interactions(structure_id, q, t, qc, tc, inter, intra, level, distance_cutoff, bio=None, qindex=0, tindex=0):
    tree_q = cKDTree(qc)
    tree_t = cKDTree(tc)

    sparse_dm = tree_t.sparse_distance_matrix(tree_q, max_distance=distance_cutoff, output_type='dict')

    # Add interactions to rows.
    # There are redundant interactions when aggregating the results at the 'chain' and 'group' level,
    # since multiple atoms in a group may be involved in interactions.
    # Therefore we use a set of rows to store only unique interactions.
    if level == 'atom' or level == 'coord':
        rows = list()
    else:
        rows = set()

    for ind, dis in sparse_dm.items():
        # exclude self interactions (this can happen if the query and target criteria overlap)
        if dis < 0.001:
            continue

        i = ind[0]  # polymer target atom index
        j = ind[1]  # polymer query atom index

        tr = t.iloc[[i]]
        qr = q.iloc[[j]]

        qcid = qr['chain_id'].item()
        tcid = tr['chain_id'].item()

        qgn = qr['group_number'].item()
        tgn = tr['group_number'].item()

        if bio is None:
            id = structure_id + "." + tr['chain_name'].item()

            # handle intra vs inter-chain interactions
            if qcid == tcid:
                 # cases with interactions in the same chain
                if not intra:
                # exclude intrachain interactions
                    continue
                if qgn == tgn:
                # exclude interactions within the same chain and group
                    continue
            else:
                # case with interactions in different chains
                if not inter:
                    # exclude inter-chain interactions
                    continue

        else:
            if qindex == tindex and qcid == tcid and qgn == tgn:
                # exclude interactions within the same chain, transform, and group
                continue

            id = structure_id + '.' + tr['chain_name'].item() + '-' + str(qindex) + ':' + str(tindex)

        # if level == 'chain':
        #     # row = Row(id,  # structureChainId
        #     #             qr['chain_name'].item(),  # queryChainId
        #     #             qr['group_name'].item(),  # queryGroupId
        #     #             qr['group_number'].item(),  # queryGroupNumber
        #     #             tr['chain_name'].item()  # targetChainId
        #     #           )
        #     row = (id,  # structureChainId
        #             qr['chain_name'].item(),  # queryChainId
        #             qr['group_name'].item(),  # queryGroupId
        #             qr['group_number'].item(),  # queryGroupNumber
        #             tr['chain_name'].item()
        #            )
        #     rows.add(row)
        #
        # elif level == 'group':
        #     row = Row(id,  # structureChainId
        #                 qr['chain_name'].item(),  # queryChainId
        #                 qr['group_name'].item(),  # queryGroupId
        #                 qr['group_number'].item(),  # queryGroupNumber
        #                 tr['chain_name'].item(),  # targetChainId
        #                 tr['group_name'].item(),  # targetGroupId
        #                 tr['group_number'].item(),  # targetGroupNumber
        #               )
        #     rows.add(row)
        #
        # elif level == 'atom':
        #     row = Row(id,  # structureChainId
        #                 qr['chain_name'].item(),  # queryChainId
        #                 qr['group_name'].item(),  # queryGroupId
        #                 qr['group_number'].item(),  # queryGroupNumber
        #                 qr['atom_name'].item(),  # queryAtomName
        #                 tr['chain_name'].item(),  # targetChainId
        #                 tr['group_name'].item(),  # targetGroupId
        #                 tr['group_number'].item(),  # targetGroupNumber
        #                 tr['atom_name'].item(),  # targetAtomName
        #                 dis,  # distance
        #                 )
        #     rows.append(row)

        # -----
        # add query data
        row = (id, qr['chain_name'].item())
        if bio is not None:
            row += (qindex,)
        row += (qr['group_name'].item(),  qgn)
        if level == 'atom':
            row += (qr['atom_name'].item(),)

        # add target data
        row += (tr['chain_name'].item(),)
        if bio is not None:
            row += (tindex,)
        if level == 'group' or level == 'atom' or level == 'coord':
            row += (tr['group_name'].item(), tgn)
        if level == 'atom' or level == 'coord':
            row += (tr['atom_name'].item(), dis)
        if level == 'coord':
            rows += (qc[j][0], qc[j][1], qc[j][2], tc[i][0], tc[i][1], tc[i][2])

        # add row
        if level == 'atom' or level == 'coord':
            rows.append(row)
        else:
            # add to a set to remove redundant info at the group or chain level
            rows.add(row)

    return list(rows)
