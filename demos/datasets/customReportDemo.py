#!/usr/bin/env python
'''
customReportDemo.py:

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.datasets import customReportService
import time


def main():
    start = time.time()

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("secondaryStructureSegmentDemo")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    # retrive PDB annotation: Binding affinities (Ki, Kd)
    # group name of the ligand (hetId), and the
    # Enzyme Classification number (ecNo)
    ds = customReportService.getDataset(["Ki","Kd","hetId","ecNo"])

    # Show schema of this dataset
    ds.printSchema()

    # Select structures that either have Ki or Kd value(s) and
    # are protein-serine/threonine kinases (EC 2.7.1.*):

    # A. By using dataset operations
    ds = ds.filter("(Ki IS NOT NULL OR Kd IS NOT NULL) AND ecNo LIKE '2.7.11.%'")
    ds.show(10)

    # B. by creating a temporary query and running SQL
    ds.createOrReplaceTempView("table")
    ds = sqlContext.sql("SELECT * from table WHERE (Ki IS NOT NULL OR Kd IS NOT NULL) AND ecNo LIKE '2.7.11.%'")
    ds.show(10)

    end = time.time()

    print("Time: %f  sec." %(end-start))

    sc.stop()

if __name__ == "__main__":
    main()
