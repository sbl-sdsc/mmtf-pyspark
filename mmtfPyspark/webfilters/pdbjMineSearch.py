#!/user/bin/env python
'''PdbjMineSearch.py

This filter runs an PDBj Mine 2 Search web service using an SQL query.

References
----------
- Each category represents a table, and fields represent database columns, see
available tables and columns: `MINE RDB DOCS <https://pdbj.org/mine-rdb-docs>`_

- Data are provided through: `MINE2-SQl <https://pdbj.org/help/mine2-sql>`_

- Queries can be designed with the interactive PDBjMine2 query service: `PDBjMine2 SQL <https://pdbj.org/mine/sql>`_
'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import urllib
import tempfile
from pyspark.sql import SparkSession
from mmtfPyspark.datasets import pdbjMineDataset
from urllib.request import urlretrieve
import requests


class PdbjMineSearch(object):
    '''Fetch data using the PDBj Mine 2 SQL service

    Attributes
    ----------
    sqlQuery: str
       the sql query [None]
    '''

    URL = "https://pdbj.org/rest/mine2_sql"

    def __init__(self, sqlQuery):

        self.chainLevel = False
        self.pdbIds = []

        dataset = pdbjMineDataset.get_dataset(sqlQuery)

        if dataset == None:
            raise Exception(
                "Dataset empty. Either provide an sql query or a dataset")

        # Check if there is a pdbID file
        if 'structureId' in dataset.columns:
            self.chainLevel = False
            self.pdbIds = [a[0] for a in dataset.select('structureId').collect()]

        if 'structureChainId' in dataset.columns:
            self.chainLevel = True
            ids = [a[0] for a in dataset.select('structureChainId').collect()]
            ids_sub = [i[:4] for i in ids]
            self.pdbIds = ids + ids_sub

    def __call__(self, t):
        match = t[0] in self.pdbIds

        # If results are PDB IDs. but the keys contains chain names,
        # then trucate the chain name before matching (eg. 4HHB.A -> 4HHB)
        if not match and not self.chainLevel and len(t[0]) > 4:
            match = t[0][:4] in self.pdbIds

        return match
