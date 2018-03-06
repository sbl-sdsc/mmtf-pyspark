#!/user/bin/env python
'''PdbjMine.py

This filter runs an PDBj Mine 2 Search web service using SQL query

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
from urllib.request import urlretrieve
import requests


class PdbjMine(object):
    '''Fetch data using the PDBj Mine 2 SQL service

    Attributes
    ----------
        sqlQuery (str): the sql query [None]
        dataset: csv file of the target dataset [None]
        pdbidField (str): field name of pdbIds [pdbid]
        clainLevel (bool): flag to indicate chain level query
    '''

    URL = "https://pdbj.org/rest/mine2_sql"

    def __init__(self, sqlQuery=None, dataset=None, pdbidField="pdbid",
                 chainLevel=False):

        self.pdbidField = pdbidField
        self.chainLevel = chainLevel
        self.sqlQuery = sqlQuery

        if self.sqlQuery != None:

            encodedSQL = urllib.parse.quote(self.sqlQuery)
            tmp = tempfile.NamedTemporaryFile(delete=False)

            # URL retrive won't work on certain IP address
            urlretrieve(self.URL + "?format=csv&q=" + encodedSQL, tmp.name)

            spark = SparkSession.builder.getOrCreate()

            # TODO: Using self.dataset causes spark unable to use filter
            dataset = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("parserLib", "UNIVOCITY") \
                .load(tmp.name)

        if dataset == None:
            raise Exception(
                "Dataset empty. Either provide an sql query or a dataset")

        # Check if there is a pdbID file
        if self.pdbidField in dataset.columns:

            self.pdbIds = [a[0].upper()
                           for a in dataset.select(self.pdbidField).collect()]

    def __call__(self, t):
        match = t[0] in self.pdbIds

        # If results are PDB IDs. but the keys contains chain names,
        # then trucate the chain name before matching (eg. 4HHB.A -> 4HHB)
        if not match and not self.chainLevel and len(t[0]) > 4:
            match = t[0][:4] in self.pdbIds

        return match
