#!/user/bin/env python
"""
advancedSearchDataset.py:

Creates a dataset of polymer sequences using the full sequence
used in the experiment (i.e., the "SEQRES" record in PDB files).
"""
__author__ = "Peter Rose"
__maintainer__ = "Peter Rose"
__email__ = "pwrose@ucsd.edu"
__version__ = "0.2.0"
__status__ = "Debug"

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, explode, split, substring_index

from mmtfPyspark.datasets import pdbjMineDataset
from mmtfPyspark.webservices.advancedQueryService import post_query


def get_dataset(xmlQuery):
    """
    Runs an RCSB PDB Advanced Search web service using an XML query description.
    See https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedSearch.html Advanced Search
    Returns a dataset of structureIds or structureChainIds based
    on the specified query.

    :param xmlQuery: RCSB PDB advanced query xml string
    :return: dataset with matching ids
    """

    # run advanced query
    ids = post_query(xmlQuery)

    # convert list of ids to a list of lists (required for dataframe creation below)
    results = []
    for i in ids:
        results.append([i])

    # convert list of lists to a dataframe
    spark = SparkSession.builder.getOrCreate()
    ds: DataFrame = spark.createDataFrame(results, ['structureEntityId'])
    ds = ds.withColumn("structureId", substring_index(ds.structureEntityId, ':', 1))

    # if results contain an entity id, e.g., 101M:1, then map entityId to structureChainIds
    if len(results[0][0]) > 4:
        ds = ds.withColumn("entityId", substring_index(ds.structureEntityId, ':', -1))
        mapping = get_entity_to_chain_id()
        ds = ds.join(mapping, (ds.structureId == mapping.structureId) & (ds.entityId == mapping.entity_id))
        ds = ds.select(ds.structureChainId)
    else:
        ds = ds.select(ds.structureId)

    return ds


def get_entity_to_chain_id():
    # get entityID to strandId mapping
    query = "SELECT pdbid, entity_id, pdbx_strand_id FROM entity_poly"
    mapping: DataFrame = pdbjMineDataset.get_dataset(query)

    # split one-to-many relationship into multiple records: 'A,B -> [A, B] -> explode to separate rows
    mapping = mapping.withColumn("chainId", split(mapping.pdbx_strand_id, ","))
    mapping = mapping.withColumn("chainId", explode("chainId"))

    # create a structureChainId file, e.g. 1XYZ + A -> 1XYZ.A
    mapping = mapping.withColumn("structureChainId", concat_ws(".", mapping.structureId, mapping.chainId))

    return mapping.select(mapping.entity_id, mapping.structureId, mapping.structureChainId)
