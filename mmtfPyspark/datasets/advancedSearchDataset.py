#!/user/bin/env python
"""
advancedSearchDataset.py:

Runs an RCSB PDB Advanced Search web service using an XML query description.
See https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedSearch.html Advanced Search for
an overview and a list of available queries at
https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/index.html
The returned dataset contains the following field dependent on the query type:

# structureId, e.g., 4HHB
# structureChainId, e.g., 4HHB.A
# ligandId, e.g., HEM
"""
__author__ = "Peter Rose"
__version__ = "0.2.0"

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, explode, split, substring_index

from mmtfPyspark.datasets import pdbjMineDataset
from mmtfPyspark.webservices.advancedQueryService import post_query


def get_dataset(xmlQuery):
    """
    Runs an RCSB PDB Advanced Search web service using an XML query description.
    See https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedSearch.html Advanced Search
    The returned dataset contains the following field dependent on the query type:
    # structureId, e.g., 1STP
    # structureChainId, e.g., 4HHB.A
    # ligandId, e.g., HEM

    :param xmlQuery: RCSB PDB advanced query xml string
    :return: dataset with matching ids
    """

    # run advanced query
    ids = post_query(xmlQuery)

    # convert list of ids to a list of lists (required for dataframe creation below)
    id_list = [[i] for i in ids]

    # convert list of lists to a dataframe
    spark = SparkSession.builder.getOrCreate()

    # distinguish 3 types of results based on length of string
    # structureId: 4 (e.g., 4HHB)
    # structureEntityId: > 4 (e.g., 4HHB:1)
    # entityId: < 4 (e.g., HEM)

    if len(ids[0]) > 4:
        ds: DataFrame = spark.createDataFrame(id_list, ['pdbEntityId'])
        # if results contain an entity id, e.g., 101M:1, then map entityId to pdbChainId
        ds = ds.withColumn("pdbId", substring_index(ds.pdbEntityId, ':', 1))
        ds = ds.withColumn("entityId", substring_index(ds.pdbEntityId, ':', -1))
        mapping = __get_entity_to_chain_id()
        ds = ds.join(mapping, (ds.pdbId == mapping.structureId) & (ds.entityId == mapping.entity_id))
        ds = ds.select(ds.pdbChainId)
    elif len(ids[0]) < 4:
        ds: DataFrame = spark.createDataFrame(id_list, ['ligandId'])
    else:
        ds: DataFrame = spark.createDataFrame(id_list, ['pdbId'])

    return ds


def __get_entity_to_chain_id():
    # get entityID to strandId mapping
    query = "SELECT pdbid, entity_id, pdbx_strand_id FROM entity_poly"
    mapping: DataFrame = pdbjMineDataset.get_dataset(query)

    # split one-to-many relationship into multiple records: 'A,B -> [A, B] -> explode to separate rows
    mapping = mapping.withColumn("chainId", split(mapping.pdbx_strand_id, ","))
    mapping = mapping.withColumn("chainId", explode("chainId"))

    # create a structureChainId file, e.g. 1XYZ + A -> 1XYZ.A
    mapping = mapping.withColumn("pdbChainId", concat_ws(".", mapping.structureId, mapping.chainId))

    return mapping.select(mapping.entity_id, mapping.structureId, mapping.pdbChainId)
