'''customReportQuery.py

This filter runs an SQL query on specified PDB metadata and annotation fields retrived using
RCSB PDB RESTful web services. The fields are then queried and the resulting PDB IDs are
used to filter the data. The input to the filter consists of an SQL WHERE clause, and list
data columns availible from RCSB PDB web services.

References
----------
- List of supported field names: `reportFiled <http://www.rcsb.org/pdb/results/reportField.do>`_
- Examples of SQL WHERE clauses: `SQL where <https://www.w3schools.com/sql/sql_where.asp>`_

Examples
--------
Find PDB entries with Enzyme classification number 2.7.11.1
and source organism Homo sapiens:

>>> pdb = read_full_sequence_files(sc)
>>> whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'"
>>> pdb = pdb.filter(RcsbWebserviceFilter(whereClause, "ecNo","source"))

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__status__ = "Done"

from mmtfPyspark.datasets import customReportService
from pyspark.sql import SparkSession


class CustomReportQuery(object):
    '''Filters using an SQL query on the specified fields

    Attributes
    ----------
    whereClause : str
       WHERE Clause of SQL statement

    fields : str, list
       one or more field names to be used in query
    '''

    def __init__(self, whereClause, fields):

        # Check if fields are in a list or string
        if type(fields) == str:

            if ',' in fields:
                fields = fields.split(',')

            else:
                fields = [fields]

        # Get requested data columns
        dataset = customReportService.get_dataset(fields)

        # Check if the results contain chain level data
        self.chainLevel = "structureChainId" in dataset.columns

        # Create a temporary view of the dataset
        dataset.createOrReplaceTempView("table")

        # Create SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Run SQL query
        if (self.chainLevel):
            # For chain level data
            sql = "SELECT structureChainID, structureId, chainId FROM table " \
                  + whereClause
            results = spark.sql(sql)

            # Add both PDB entry and chain level data, so chain-based data can be filtered
            self.pdbIds = results.distinct().rdd.map(lambda x: x[0]).collect()
            self.pdbIds += results.distinct().rdd.map(lambda x: x[1]).collect()

        else:
            # For PDB entry level data
            sql = "SELECT structureId FROM table " + whereCaluse
            results = spark.sql(sql)
            self.pdbIds = results.distinct().rdd.map(lambda x: x[0]).collect()

        self.pdbIds = list(set(self.pdbIds))

    def __call__(self, t):

        match = t[0] in self.pdbIds

        # If results are PDB IDs, but the keys contains chain names,
        # Then truncate the chain name before matching (e.g., 4HHB.A -> 4HHB)
        if (not self.chainLevel) and (not match) and (len(t[0]) > 4):
            print(t[0])
            return t[0][:4] in self.pdbIds

        return match
