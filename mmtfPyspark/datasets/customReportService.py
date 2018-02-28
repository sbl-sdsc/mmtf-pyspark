#!/user/bin/env python
'''
customReportService.py

TODO: wtf is wrong with this docstring


ThisclassusesRCSBPDBTabularReportRESTfulwebServicestoretrievemetadata
andannotationsforallcurrententriesintheProteinDataBank.
See<ahref="http://www.rcsb.org/pdb/results/reportField.do">forlistofsupported
fieldnames.</a>
<p>Reference:TheRCSBProteinDataBank:redesignedwebsiteandwebServices2011
NucleicAcidsRes.39:D392-D401.
See<ahref="https://dx.doi.org/10.1093/nar/gkq1021">doi:10.1093/nar/gkq1021</a>

<p>Example:RetrievePubMedCentral,PubMedID,andDepositiondate
<pre>
{@code
Dataset<Row>ds=CustomReportService.getDataset("pmc","pubmedId","depositionDate");
ds.printSchema();
ds.show(5);
}
</pre>

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

import urllib
from urllib import request
import tempfile
from pyspark.sql import SparkSession


SERVICELOCATION="http://www.rcsb.org/pdb/rest/customReport"
CURRENT_URL = "?pdbids=*&service=wsfile&format=csv&primaryOnly=1&customReportColumns="

def getDataset(columnNames):
    '''
    Returns a dataset with the specified columns for all current PDB entires.
    See <a href="https://www.rcsb.org/pdb/results/reportField.do">  for a list
    of supported filed names

    Attributes:
        columnNames: names of columns for the dataset

    Returns:
        dataset with the specified columns
    '''

    if type(columnNames) == str:
        columnNames = [columnNames]

    query = CURRENT_URL + ','.join(columnNames)
    inStream = postQuery(query)

    tmp = tempfile.NamedTemporaryFile(delete=False)

    with open(tmp.name, "w") as t:
        for l in inStream:
            t.writelines(str(l)[2:-3] + '\n')

    spark = SparkSession.builder.getOrCreate()

    dataset = readCsv(spark, tmp.name)

    return concatIds(spark, dataset, columnNames)


def concatIds(spark, dataset, columnNames):
    '''
    Concatenates structureId and chainId fields into a single key if chainId
    field is present

    Attributes:
        spark (SparkSession)
        dataset (Dataframe)
        columnNames (list): columnNames
    '''

    if "chainId" in dataset.columns:
        dataset.createOrReplaceTempView("table")

        sql = "SELECT CONCAT(structureId,'.',chainId) as structureChainId," + \
              "structureId,chainId,%s"%','.join(columnNames) + \
              " from table"

        dataset = spark.sql(sql)

    return dataset


def postQuery(query):
    '''
    Post PDB Ids and fields in a query string to the RESTful RCSB web service

    Attributes:
        query (string): RESTful query urlopen

    Returns:
        input stream to response
    '''

    encodedQuery = urllib.parse.quote(query).encode('utf-8')
    url = request.Request(SERVICELOCATION)
    stream = urllib.request.urlopen(url, data = encodedQuery)

    return stream


def readCsv(spark, inputFileName):
    '''
    Reads CSV file into a Spark dataset
    '''

    dataset = spark.read \
                   .format("csv") \
                   .option("header", "true") \
                   .option("inferSchema", "true") \
                   .load(inputFileName) \
                   .cache()

    return dataset
