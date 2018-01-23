#!/user/bin/env python
'''
mineSearch.py

This filter runs an PDBj Mine 2 Search web service using SQL query

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


def getDataset(sqlQuery):

    encodedSQL = urllib.parse.quote(sqlQuery)
    tmp = tempfile.NamedTemporaryFile(delete=False)

    URL= "https://pdbj.org/rest/mine2_sql"
    urlretrieve(URL + "?format=csv&q=" + encodedSQL, tmp.name)

    spark = SparkSession.builder.getOrCreate()

    dataset = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("parserLib", "UNIVOCITY") \
        .load(tmp.name)

    return dataset
