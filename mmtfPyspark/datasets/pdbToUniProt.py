#!/user/bin/env python
"""
Downloads PDB to UniProt chain and residue-level mappings from the SIFTS project.
Data are retrieved from a data cache and only data that are not cached
are downloaded when required.
This class also provides a method to create a new mapping file.

For more information about SIFTS see:
The "Structure Integration with Function, Taxonomy and Sequence"
https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html is
the authoritative source of up-to-date chain and residue-level
mappings to UniProt.

Example of chain-level mappings

>>> df = get_chain_mappings()
>>> df.show()

+----------------+-----------+-------+---------+
|structureChainId|structureId|chainId|uniprotId|
+----------------+-----------+-------+---------+
|          1A02.F|       1A02|      F|   P01100|
|          1A02.J|       1A02|      J|   P05412|
|          1A02.N|       1A02|      N|   Q13469|

Example of residue-level mappings

>>> df = get_cached_residue_mappings()
>>> df.show()

Columns:
structureChainId - pdbId.chainId
pdbResNum - PDB residue number in ATOM records
pdbSeqNum - PDB residue number in the sequence record (index start at 1)
uniprotId - UniProt id (accession number)
uniprotNum - UniProt residue number (index starts at 1)

+----------------+---------+---------+---------+----------+
|structureChainId|pdbResNum|pdbSeqNum|uniprotId|uniprotNum|
+----------------+---------+---------+---------+----------+
|          1STP.A|     null|        1|   P22629|        25|
|          1STP.A|     null|        2|   P22629|        26|
|          1STP.A|     null|        3|   P22629|        27|
|          1STP.A|     null|       12|   P22629|        36|
 ...
|          1STP.A|       13|       13|   P22629|        37|
|          1STP.A|       14|       14|   P22629|        38|
|          1STP.A|       15|       15|   P22629|        39|
 ...
"""

__author__ = "Peter Rose"
__version__ = "0.2.0"

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, upper

# location of cached dataset (temporary location for prototyping)
CACHED_FILE_URL = "https://github.com/sbl-sdsc/mmtf-data/raw/master/data/pdb2uniprot_residues.orc.lzo"
FILENAME = "pdb2uniprot_residues.orc.lzo"

# location of SIFTS data
UNIPROT_MAPPING_URL = "http://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/csv/pdb_chain_uniprot.csv.gz"
UNIPROT_FILE = "pdb_chain_uniprot.csv.gz"
SIFTS_URL = "http://ftp.ebi.ac.uk/pub/databases/msd/sifts/split_xml/"


def get_chain_mappings(ids=None):
    """
    Returns an up-to-date dataset of PDB to UniProt
    chain-level mappings for a list of ids.
    Valid ids are either a list of pdbIds (e.g. 1XYZ) or pdbId.chainIds (e.g., 1XYZ.A).

    :param ids: list of pdbIds or pdbId.chainIds (pdbIds must be upper case, chainIds are case sensitive)
    :return: dataset of PDB to UniProt chain-level mappings
    """
    spark = SparkSession.builder.getOrCreate()

    # get a dataset of up-to-date UniProt chain mappings
    ds = __get_all_mappings()

    # return all mappings if no ids where passed in
    if not ids:
        return ds

    # convert list of ids to a list of lists (required for dataframe creation below)
    id_list = [[i] for i in ids]

    # create a dataset of ids from the passed-in list
    subset = spark.createDataFrame(id_list, ['id'])

    # create a subset of data for the list of ids
    if len(ids[0]) == 4:
        # join by pdbId
        ds = ds.join(subset, ds.pdbId == subset.id).drop("id")

    else:
        # join by pdbChainId
        ds = ds.join(subset, ds.pdbChainId == subset.id).drop("id")

    return ds


def get_cached_residue_mappings():
    """
    Returns the current version of the cached dataset of PDB to UniProt
    residue mappings. This method is faste, but may not
    contain the mapping for recently released PDB entries.
    :return: dataset of PDB to UniProt residue mappings
    """
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.orc.impl", "native")

    # download cached dataset
    spark.sparkContext.addFile(CACHED_FILE_URL)

    # read dataset
    ds = spark.read.format("orc").load(SparkFiles.get(FILENAME))

    return ds


def __get_all_mappings():
    spark = SparkSession.builder.getOrCreate()

    # download data
    spark.sparkContext.addFile(UNIPROT_MAPPING_URL)

    # read dataset
    ds = spark.read.option("header", "true") \
        .option("comment", "#") \
        .option("inferSchema", "true") \
        .csv(SparkFiles.get(UNIPROT_FILE))

    # cleanup and rename columns to be consistent with MMTF conventions.
    ds = ds.withColumn("PDB", upper(ds.PDB)) \
        .withColumnRenamed("PDB", "pdbId") \
        .withColumnRenamed("CHAIN", "chainId") \
        .withColumnRenamed("SP_PRIMARY", "uniProtId") \
        .withColumn("pdbChainId", concat_ws(".", "pdbId", "chainId")) \
        .select("pdbChainId", "pdbId", "chainId", "uniProtId")

    return ds
