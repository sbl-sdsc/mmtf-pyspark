#!/user/bin/env python
'''pdbjMineDataset.py

This filter runs an PDBj Mine 2 Search web service using SQL query

References
----------
    Data are provided through Mine2 SQL
        https://pdbj.org/help/mine2-sql
    Queries can be designed with the interactive PDBj Mine 2 query service:
        https://pdbj.org/mine/sql
    PDB metadata are described in the PDB mmCIF Dictionary:
        http://mmcif.wwpdb.org/

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com"
    __version__ = "0.2.0"
    __status__ = "Done"
'''

import urllib
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, upper, concat
from urllib.request import urlretrieve
import requests


def get_dataset(sqlQuery):
    '''Runs a PDBj Mine 2 search web service using an SQL query

    Attributes
    ----------
        sqlQuery (str): the sql query for the web service
    '''

    encodedSQL = urllib.parse.quote(sqlQuery)
    tmp = tempfile.NamedTemporaryFile(delete=False)

    URL = "https://pdbj.org/rest/mine2_sql"
    urlretrieve(URL + "?format=csv&q=" + encodedSQL, tmp.name)

    spark = SparkSession.builder.getOrCreate()

    ds = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("parserLib", "UNIVOCITY") \
        .load(tmp.name)

    # rename.concatenate columns to assign
    # consistent primary keys to datasets
    if "pdbid" in ds.columns:
        # this project uses upper case pdbids
        ds = ds.withColumn("pdbid", upper(col("pdbid")))

        if "chain" in ds.columns:
            ds = ds.withColumn("structureChainId", \
                               concat(col("pdbid"), lit("."), col("chain")))
            ds.drop("pdbid","chain")
        else:
            ds = ds.withColumnRenamed("pdbid", "structureId")

    return ds
