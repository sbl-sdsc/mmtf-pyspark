#!/user/bin/env python
__author__ = "Peter Rose"
__version__ = "0.2.0"

from pyspark import SparkFiles
from pyspark.sql import SparkSession

# location of cached dataset (temporary location for prototyping)
CACHED_FILE_URL = "https://github.com/sbl-sdsc/mmtf-data/raw/master/data/nsSNP3D_PDB_UNP_GRCh37_t9606_s95.lzo.orc"
FILENAME = "nsSNP3D_PDB_UNP_GRCh37_t9606_s95.lzo.orc"


def get_cached_dataset():
    """
    Returns a dataset of missense mutations in dbSNP mapped to UniProt and PDB residue positions.

    The dataset contains the following columns:
    +---+---------+---------+------------+---------+----------+----------+----------+---------+-------+-------+ ...
    |chr|      pos|   snp_id|  master_acc|master_gi|master_pos|master_res|master_var|   pdb_gi|pdb_res|pdb_pos|
    +---+---------+---------+------------+---------+----------+----------+----------+---------+-------+-------+ ...
    |  4| 79525461|764726341|   NP_005130|  4826643|       274|         R|         *|157829892|      R|    274|
    |  4| 79525462|771966889|   NP_005130|  4826643|       274|         R|         P|157829892|      R|    274| ...

    ... +-----------+--------------------+----------+------+---------+---------+----------+
        |blast_ident|             clinsig|pdbChainId|tax_id|pdbResNum|uniprotId|uniprotNum|
    ... +-----------+--------------------+----------+------+---------+---------+----------+
        |      100.0|                null|    1AII.A|  9606|      275|   P12429|       274|
    ... |      100.0|                null|    1AII.A|  9606|      275|   P12429|       274|

    Reference:
    dbSNP: https://www.ncbi.nlm.nih.gov/projects/SNP/


    :return: dataset of missense mutations in dbSNP
    """
    spark = SparkSession.builder.getOrCreate()

    # download cached dataset
    spark.sparkContext.addFile(CACHED_FILE_URL)

    # read dataset
    spark.conf.set("spark.sql.orc.impl", "native")
    return spark.read.orc(SparkFiles.get(FILENAME))
