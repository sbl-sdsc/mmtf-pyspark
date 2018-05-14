#!/user/bin/env python
'''g2sDataset.py

This class maps human genetic variation positions to PDB structure positions.
Genomic positions must be specified for the hgvs-grch37 reference genome using
the HGVS sequence variant nomenclature.

References
----------
- HGVS sequence variant nomenclature http://varnomen.hgvs.org/
- G2S Web Services https://g2s.genomenexus.org/
- Juexin Wang, Robert Sheridan, S Onur Sumer, Nikolaus Schultz, Dong Xu, Jianjiong Gao; 
  G2S: a web-service for annotating genomic variants on 3D protein structures (2018) 
  Bioinformatics. https://doi.org/10.1093/bioinformatics/bty047

Examples
--------
>>> variantIds = ["chr7:g.140449098T>C", "chr7:g.140449100T>C"]
>>> ds = g2sDataset.get_position_dataset(variantIds, "3TV4", "A")
>>> ds.show()
+-----------+-------+-----------+------------+-----------+-------------------+
|structureId|chainId|pdbPosition|pdbAminoAcid|  refGenome|        variationId|
+-----------+-------+-----------+------------+-----------+-------------------+
|       3TV4|      A|        661|           N|hgvs-grch37|chr7:g.140449098T>C|
|       3TV4|      A|        660|           N|hgvs-grch37|chr7:g.140449100T>C|
+-----------+-------+-----------+------------+-----------+-------------------+

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from pyspark.sql.functions import col, explode, upper
from pyspark.sql import SparkSession
from pyspark import SparkContext
from io import BytesIO
import requests

REFERENCE_GENOME = "hgvs-grch37"
G2S_REST_URL = "https://g2s.genomenexus.org/api/alignments/"+REFERENCE_GENOME+"/"


def get_position_dataset(variationIds, structureId = None, chainId = None):
    '''Downloads PDB residue mappings for a list of genomic variations

    Parameters
    ----------
    variationIds : list
       genomic variation ids, e.g. chr7:g.140449103A>C
    structureId : str
       specific PDB structure used for mapping [None]
    chainId : str
       specific chain used for mapping [None]

    Returns
    -------
    dataset
       dataset with PDB mapping information
    '''

    dataset = _get_dataset(variationIds, structureId, chainId)

    if dataset == None: return None

    cols = ["structureId","chainId","pdbPosition","pdbAminoAcid","refGenome",\
            "variationId"]
    return dataset.select(cols).distinct()


def get_full_dataset(variationIds, structureId = None, chainId = None):
    '''Downloads PDB residue mappings and alignment information for a list of
    genomic variations

    Parameters
    ----------
    variationIds : list
       genomic variation ids, e.g. chr7:g.140449103A>C
    structureId : str
       specific PDB structure used for mapping [None]
    chainId : str
       specific chain used for mapping [None]

    Returns
    -------
    dataset
       dataset with PDB mapping information
    '''

    return _get_dataset(variationIds, structureId, chainId)


def _get_dataset(variationIds, structureId, chainId):
    '''Downloads PDB residue mappings for a list of genomic variations

    Parameters
    ----------
    variationIds : list
       genomic variation ids, e.g. chr7:g.140449103A>C
    structureId : str
       specific PDB structure used for mapping
    chainId : str
       specific chain used for mapping

    Returns
    -------
    dataset
       dataset with PDB mapping information
    '''

    # Get a spark context
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # Download data in parallel
    data = sc.parallelize(variationIds) \
             .flatMap(lambda m: _get_data(m, structureId, chainId))

    # Convert Python Rdd to dataframe
    dataframe = spark.read.json(data)

    # Returns None if dataframe is empty
    if len(dataframe.columns) == 0:
        print("g2sDataset: no match found")
        return None

    dataframe = _standardize_data(dataframe)

    return _flatten_dataframe(dataframe)


def _get_data(variationId, structureId, chainId):
    '''Downloads PDB residue mappings for a list of genomic variations'''
    data = []

    if structureId is None and chainId is None:
        url = G2S_REST_URL + variationId + '/residueMapping'
    elif structureId is not None and chainId is not None:
        url = G2S_REST_URL + variationId + '/pdb/' + structureId + '_' \
              + chainId + '/residueMapping'
    else:
        raise Exception("Both structureId and chainId have to be provided")

    try:
        req = requests.get(url)
    except:
        print(f"WARNING: could not load data for: {variationId}")
        return data

    if b"\"error\":\"Not Found\"" in req.content:
        print(f"WARNING: could not load data for: {variationId}")
        return data

    results = [inputStream.decode() for inputStream in BytesIO(req.content).readlines()]

    if results is not None and len(results[0]) > 100:
        results = _add_variant_id(results, REFERENCE_GENOME, variationId)
        data = results

    return data


def _add_variant_id(json, refGenome, variationId):
    '''Adds reference genome and variationId to each json records

    Parameters
    ----------
    json : list
       list of original json strings
    refGenome 
       reference genome
    variationId : str
       variation identifier
    '''

    ids = "\"refGenome\":\"" + refGenome + "\"," + "\"variationId\":\"" \
          + variationId + "\"," + "\"alignmentId\""
    return [j.replace("\"alignmentId\"", ids) for j in json]


def _standardize_data(df):
    '''Standardize data and column names to be consistent'''
    return df.withColumn("structureId", upper(col("pdbId"))) \
             .withColumnRenamed("chain", "chainId")


def _flatten_dataframe(df):
    return df.withColumn("pdbPosition", explode(col("residueMapping.pdbPosition"))) \
             .withColumn("pdbAminoAcid", explode(col("residueMapping.pdbAminoAcid")))
