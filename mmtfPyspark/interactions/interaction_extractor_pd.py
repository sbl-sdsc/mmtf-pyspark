#!/user/bin/env python
'''interaction_extractor.py

This class calculates pairwise intra- and inter-molecular interactions at specified
levels of granularity within biological assemblies and asymmetric units.

'''
__author__ = "Peter W Rose"
__version__ = "0.3.6"
__status__ = "experimental"

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from mmtfPyspark.utils import ColumnarStructure
import numpy as np
from scipy.spatial import cKDTree


class InteractionExtractorPd(object):

    @staticmethod
    def get_interactions(structure, distance_cutoff=4.0, query=None, target=None, inter=True, intra=False, bio=1, level='group'):
        '''Return a dataframe of pairwise interactions

        The dataframe contains some or all of the following columns depending on the specified level.
        - structureChainId - pdbId.chainName of interacting chain
        - q_chain_name -  chain name of the query
        - q_trans - id of bio assembly transformation applied to query
        - q_group_name - id of the query group (residue) from the PDB chemical component dictionary
        - q_group_number - group number of the query group (residue) including insertion code (e.g. 101A)
        - q_atom_name - atom name of the query atom
        - t_chain_name" - chain name of the target
        - t_trans - id of bio assembly transformation applied to target
        - t_group_name - id of the target group (residue) from the PDB chemical component dictionary
        - t_group_number - group number of the target group (residue) including insertion code (e.g. 101A)
        - t_atom_name - atom name of the target atom
        - distance - distance between interaction atoms
        - q_x, q_y, q_z - coordinates of query atoms
        - t_x, t_y, t_z - coordinates of target atoms

        Parameters
        ----------
        structure : mmtf structure
        distance_cutoff : distance threshold for interactions
        query : Pandas query string to select 'query' atoms
        target: Pandas query string to select 'target' atoms
        inter: calculate intermolecular interaction if True
        intra: calculate intramolecular interactions if True
        bio : biological assembly number: None for asymmetric unit or 1, 2, ... for bio assembly
        level : 'chain', 'group', 'atom', or 'coord' granularity level at which to aggregate results

        Returns
        -------
        dataframe
           Spark dataframe with pairwise interaction information
        '''

        # find all interactions
        if bio is None:
            rows = structure.flatMap(AsymmetricUnitInteractions(query, target, distance_cutoff, inter, intra, level))
        else:
            rows = structure.flatMap(BioAssemblyInteractions(query, target, distance_cutoff, inter, intra, bio, level))

        # TODO consider adding parameters
        # only hetero or homo interactions
        # chem: add element, entity_type(LGO, PRO, DNA, etc.)
        # seq=True -> add sequence index, sequence
        # geom=True -> add distance, order parameters([q3,q4,q5,q6]

        # Convert RDD of rows to a dataset using a schema
        spark = SparkSession.builder.getOrCreate()
        schema = InteractionExtractorPd._get_schema(level, bio)
        return spark.createDataFrame(rows, schema)

    @staticmethod
    def _get_schema(level, bio):
        fields = []
        nullable = False

        fields.append(StructField("structure_chain_id", StringType(), nullable))

        # define query columns
        fields.append(StructField("q_chain_name", StringType(), nullable))
        if bio is not None:
            fields.append(StructField("q_trans", IntegerType(), nullable))

        if level != 'chain':
            fields.append(StructField("q_group_number", StringType(), nullable))
            fields.append(StructField("q_group_name", StringType(), nullable))

            if level == 'atom' or level == 'coord':
                fields.append(StructField("q_atom_name", StringType(), nullable))

        # define target columns
        fields.append(StructField("t_chain_name", StringType(), nullable))
        if bio is not None:
            fields.append(StructField("t_trans", IntegerType(), nullable))

        if level != 'chain':
            fields.append(StructField("t_group_number", StringType(), nullable))
            fields.append(StructField("t_group_name", StringType(), nullable))

            if level == 'atom' or level == 'coord':
                fields.append(StructField("t_atom_name", StringType(), nullable))
                fields.append(StructField("distance", FloatType(), nullable))

                if level == 'coord':
                    fields.append(StructField("q_x", FloatType(), nullable))
                    fields.append(StructField("q_y", FloatType(), nullable))
                    fields.append(StructField("q_z", FloatType(), nullable))
                    fields.append(StructField("t_x", FloatType(), nullable))
                    fields.append(StructField("t_y", FloatType(), nullable))
                    fields.append(StructField("t_z", FloatType(), nullable))

        schema = StructType(fields)
        return schema


class AsymmetricUnitInteractions:

    def __init__(self, query, target, distance_cutoff, inter, intra, level):
        self.query = query
        self.target = target
        self.distance_cutoff = distance_cutoff
        self.inter = inter
        self.intra = intra

        self.level = level

    def __call__(self, t):
        structure_id = t[0]

        # Get a pandas dataframe representation of the structure
        #structure = ColumnarStructure(t[1])
        structure = t[1]

        df = structure.to_pandas()

        if df is None:
            return []

        # Apply query filter
        if self.query is None:
            q = df
        else:
            q = df.query(self.query)

        if q is None or q.shape[0] == 0:
            return []

        # Apply target filter
        if self.target is None:
            t = df
        elif self.target == self.query:
            # if query and target are identical, reuse the query dataframe
            t = q
        else:
            t = df.query(self.target)

        if t is None or t.shape[0] == 0:
            return []

        # group by chain ids
        q_chains = q.groupby('chain_id')
        t_chains = t.groupby('chain_id')

        rows = list()

        # Find interactions between pairs of chains
        for q_chain in q_chains.groups.keys():
            qt = q_chains.get_group(q_chain).reset_index(drop=True)

            for t_chain in t_chains.groups.keys():

                # exclude intra interactions (same chain id)
                if not self.intra and q_chain == t_chain:
                    continue

                if not self.inter and q_chain != t_chain:
                    continue

                tt = t_chains.get_group(t_chain).reset_index(drop=True)

                # Stack coordinates into an nx3 array
                cq = np.column_stack((qt['x'].values, qt['y'].values, qt['z'].values)).copy()
                ct = np.column_stack((tt['x'].values, tt['y'].values, tt['z'].values)).copy()

                rows += _calc_interactions(structure_id, qt, tt, cq, ct, self.level,
                                           self.distance_cutoff, None, -1, -1)

        return rows


class BioAssemblyInteractions:

    def __init__(self, query, target, distance_cutoff, inter, intra, bio, level):
        self.query = query
        self.target = target
        self.distance_cutoff = distance_cutoff
        self.inter = inter
        self.intra = intra
        self.bio = bio
        self.level = level

    def __call__(self, t):
        structure_id = t[0]

        if self.bio < 1:
            raise ValueError('bio assembly number must be >= 1, was:', self.bio)

        # if the specified bio assembly does not exist, return an empty list
        if len(t[1].bio_assembly) < self.bio:
            return []

        #structure = ColumnarStructure(t[1])
        structure = t[1]

        # Get a pandas dataframe representation of the structure
        df = structure.to_pandas()
        if df is None:
            return []

        # Apply query filter
        if self.query is None:
            q = df
        else:
            q = df.query(self.query)

        if q is None or q.shape[0] == 0:
            return []

        # Apply target filter
        if self.target is None:
            t = df
        elif self.target == self.query:
            # if query and target are identical, reuse the query dataframe
            t = q
        else:
            t = df.query(self.target)

        if t is None or t.shape[0] == 0:
            return []

        # Group by chain ids
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

            # Stack coordinates into an nx3 array
            cq = np.column_stack((qt['x'].values, qt['y'].values, qt['z'].values)).copy()
            # Create transformation matrix
            qmat = np.array(q_transform[2]).reshape((4, 4))

            # Apply bio assembly transformations
            #   apply rotation
            cqt = np.matmul(cq, qmat[0:3, 0:3])
            #   apply translation
            cqt += qmat[3, 0:3].transpose()

            for t_transform in transforms:
                tindex = t_transform[0]
                tchain = t_transform[1]

                # exclude intra interactions (same transformation and same chain id)
                if not self.intra and qindex == tindex and qchain == tchain:
                    continue

                if not self.inter and qindex != tindex and qchain != tchain:
                    continue

                if tchain in t_chains.groups.keys():
                    tt = t_chains.get_group(tchain).reset_index(drop=True)
                else:
                    continue

                # Stack coordinates into an nx3 array
                ct = np.column_stack((tt['x'].values, tt['y'].values, tt['z'].values)).copy()

                # Get a 4x4 transformation matrix
                tmat = np.array(t_transform[2]).reshape((4, 4))

                # Apply bio assembly transformations
                #   apply rotation
                ctt = np.matmul(ct, tmat[0:3, 0:3])
                #   apply translation
                ctt += tmat[3, 0:3].transpose()

                rows += _calc_interactions(structure_id, qt, tt, cqt, ctt, self.level,
                                           self.distance_cutoff, self.bio, qindex, tindex)

        return rows

    def get_transforms(self, col):
        """Return a dictionary of transformation index, chain indices/transformation matrices for given bio assembly"""
        trans = list()
        chain_ids = col.chain_id_list
        assembly = col.bio_assembly[self.bio - 1]  # bio assembly id are one-based
        for id, transforms in enumerate(assembly['transformList']):
            for index in transforms['chainIndexList']:
                trans.append((id, chain_ids[index], transforms['matrix']))
        return trans


def _calc_interactions(structure_id, q, t, qc, tc, level, distance_cutoff, bio, qindex, tindex):
    """Calculate distances between the two atom sets"""
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
            # exclude interactions within the same chain and group
            if qcid == tcid and qgn == tgn:
                continue
        else:
            # exclude interactions within the same chain, transform, and group
            if qindex == tindex and qcid == tcid and qgn == tgn:
                continue

        # add query data to a row
        id = structure_id + "." + tr['chain_name'].item()
        row = (id, qr['chain_name'].item(),)
        if bio is not None:
            row += (qindex,)
        if level != 'chain':
            row += (qgn, qr['group_name'].item(),)
            if level == 'atom' or level == 'coord':
                row += (qr['atom_name'].item(),)

        # add target data to a row
        row += (tr['chain_name'].item(),)
        if bio is not None:
            row += (tindex,)
        if level != 'chain':
            row += (tgn, tr['group_name'].item(),)
            if level == 'atom' or level == 'coord':
                row += (tr['atom_name'].item(), dis,)
                if level == 'coord':
                    row += (qc[j][0].item(), qc[j][1].item(), qc[j][2].item(),
                            tc[i][0].item(), tc[i][1].item(), tc[i][2].item(),)
  
        # add row
        if level == 'atom' or level == 'coord':
            # for these levels, we use a list
            rows.append(row)
        else:
            # for the group or chain level we use a set to remove redundant info
            rows.add(row)

    return list(rows)
