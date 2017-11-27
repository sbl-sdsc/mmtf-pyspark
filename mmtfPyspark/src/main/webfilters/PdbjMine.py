#!/user/bin/env python
'''
mineSearch.py

This filter runs an PDBj Mine 2 Search web service using SQL query

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

import urllib
import tempfile
from pyspark.sql import SparkSession
from urllib.request import urlretrieve
import requests


class PdbjMine(object):
    '''
    Fetch data using the PDBj Mine 2 SQL service
    '''

    URL = "https://pdbj.org/rest/mine2_sql"

    def __init__(self, sqlQuery=None, dataset=None, pdbidField="pdbid", chainLevel=False):

        self.pdbidField = pdbidField
        self.chainLevel = chainLevel
        self.sqlQuery = sqlQuery

        if self.sqlQuery != None:


            encodedSQL = urllib.parse.quote(self.sqlQuery)
            tmp = tempfile.NamedTemporaryFile(delete=False)

            # URL retrive won't work on certain IP address
            urlretrieve(self.URL + "?format=csv&q=" + encodedSQL, tmp.name)
            '''
            url = self.URL + "?format=csv&q=" + encodedSQL
            #print(requests.get(url).content)
            with open(tmp.name, 'w') as t:
                t.write(str(requests.get(url).content)[2:-1])
            '''

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
