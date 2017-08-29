#!/user/bin/env python
'''
pythonRDDToDataset.py:
Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
'''
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# TODO colNames to be string or list
def getDataset(data, colNames):
    row = data.first()
    length = len(row)

    if length != len(colNames):
        raise Exception("colNames length does not match row length")

    sf = []

    for i in range(colNames):
        o = row[i]
        if type(o) == str:
            sf.append(StructField(colNames[i], StringType(), False))
        elif type(o) == int:
            sf.append(StructField(colNames[i], IntegerType(), False))
        elif type(o) == float:
            sf.append(StructField(colNames[i], FloatType(), False))
        elif type(o) == long:
            sf.append(StructField(colNames[i], LongType(), False))
        else:
            print("Data type not implemented yet")

    schema = StuctType(sf)
    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(data, schema)







