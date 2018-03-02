#!/user/bin/env python
'''pdbjMineService.py

This filter runs an PDBj Mine 2 Search web service using SQL query

Data are provided through
    <a href="https://pdbj.org/help/mine2-sql">Mine 2 SQL</a>.

Queries can be designed with the interactive
    <a href="https://pdbj.org/mine/sql">PDBj Mine 2 query service</a>.

PDB metadata are described in the
    <a href="http://mmcif.wwpdb.org/">PDBx/mmCIF Dictionary</a>. See example

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

import urllib
import tempfile
from pyspark.sql import SparkSession
from urllib.request import urlretrieve
import requests


def get_dataset(sqlQuery):
    '''Runs a PDBj Mine 2 search web service using SQL query

    Attributes
    ----------
        sqlQuery (str): the sql query for the web service
    '''

    encodedSQL = urllib.parse.quote(sqlQuery)
    tmp = tempfile.NamedTemporaryFile(delete=False)

    URL = "https://pdbj.org/rest/mine2_sql"
    urlretrieve(URL + "?format=csv&q=" + encodedSQL, tmp.name)

    spark = SparkSession.builder.getOrCreate()

    dataset = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("parserLib", "UNIVOCITY") \
        .load(tmp.name)

    return dataset
