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

class mineSearch(object):
    '''
    Fetch data using the PDBj Mine 2 SQL service
    '''

    URL="https://pdbj.org/rest/mine2_sql"

    def __init__(self, sqlQuery, pdbidField = "pdbid", chainLevel = False):

        self.sqlQuery = sqlQuery
        self.pdbidField = pdbidField
        self.chainLevel = chainLevel

        encodedSQL = urllib.parse.quote(self.sqlQuery)
        tmp = tempfile.NamedTemporaryFile()

        urlretrieve(self.URL + "?format=csv&q=" + encodedSQL, tmp.name)

        spark = SparkSession.builder.getOrCreate()

        # TODO: Using self.dataset causes spark unable to use filter
        self.dataset = spark.read.format("csv") \
                            .option("header","true") \
                            .option("inferSchema","true") \
                            .option("parserLib","UNIVOCITY") \
                            .load(tmp.name) \
                            .cache()

        # Check if there is a pdbID filed
        if self.pdbidField in self.dataset.columns:
            self.pdbIds = [a[0].upper() for a in self.dataset.select('pdbid').collect()]


    def __call__(self, t):
        match = t[0] in self.pdbIds

        # If results are PDB IDs. but the keys contains chain names,
        # then trucate the chain name before matching (eg. 4HHB.A -> 4HHB)
        if not match and not self.chainLevel and len(t[0]) > 4:
            match = t[0][:4] in self.pdbIds

        return match
