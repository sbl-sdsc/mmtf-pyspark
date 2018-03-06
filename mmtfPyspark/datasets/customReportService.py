#!/user/bin/env python
'''customReportService.py

This class uses RCSB PDB Tabular Report RESTful webServices to retrieve
metadata and annotations for all current entries in the ProteinDataBank.

References
----------
    List of supported fieldnames:
        http://www.rcsb.org/pdb/results/reportField.do
    The RCSB Protein Data Bank:redesignedwebsiteandwebServices2011NucleicAcidsRes.39:D392-D401.
        https://dx.doi.org/10.1093/nar/gkq1021

Example
-------
    Retrieve PubMedCentral, PubMedID, and Depositiondate:
        ds = CustomReportService.getDataset("pmc","pubmedId","depositionDate")
        ds.printSchema()
        ds.show(5)

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com"
    __version__ = "0.2.0"
    __status__ = "Done"
'''

import urllib
from urllib import request
import tempfile
from pyspark.sql import SparkSession


SERVICELOCATION = "http://www.rcsb.org/pdb/rest/customReport"
CURRENT_URL = "?pdbids=*&service=wsfile&format=csv&primaryOnly=1&customReportColumns="


def get_dataset(columnNames):
    '''Returns a dataset with the specified columns for all current PDB entires.
    See <a href="https://www.rcsb.org/pdb/results/reportField.do">  for a list
    of supported filed names

    Attributes
    ----------
        columnNames: names of columns for the dataset

    Returns
    -------
        dataset with the specified columns
    '''

    if type(columnNames) == str:
        columnNames = [columnNames]

    query = CURRENT_URL + ','.join(columnNames)
    inStream = _post_query(query)

    tmp = tempfile.NamedTemporaryFile(delete=False)

    with open(tmp.name, "w") as t:
        for l in inStream:
            t.writelines(str(l)[2:-3] + '\n')

    spark = SparkSession.builder.getOrCreate()

    dataset = _read_csv(spark, tmp.name)

    return _concat_ids(spark, dataset, columnNames)


def _concat_ids(spark, dataset, columnNames):
    '''Concatenates structureId and chainId fields into a single key if chainId
    field is present

    Attributes
    ----------
        spark (SparkSession)
        dataset (Dataframe)
        columnNames (list): columnNames
    '''

    if "chainId" in dataset.columns:
        dataset.createOrReplaceTempView("table")

        sql = "SELECT CONCAT(structureId,'.',chainId) as structureChainId," + \
              "structureId,chainId,%s" % ','.join(columnNames) + \
              " from table"

        dataset = spark.sql(sql)

    return dataset


def _post_query(query):
    '''Post PDB Ids and fields in a query string to the RESTful RCSB web service

    Attributes
    ----------
        query (string): RESTful query urlopen

    Returns
    -------
        input stream to response
    '''

    encodedQuery = urllib.parse.quote(query).encode('utf-8')
    url = request.Request(SERVICELOCATION)
    stream = urllib.request.urlopen(url, data=encodedQuery)

    return stream


def _read_csv(spark, inputFileName):
    '''Reads CSV file into a Spark dataset

    Attributes
    ----------
        spark (Spark Context)
        inputFileName (str): directory path for the input file
    '''

    dataset = spark.read \
                   .format("csv") \
                   .option("header", "true") \
                   .option("inferSchema", "true") \
                   .load(inputFileName) \
                   .cache()

    return dataset
