#!/usr/bin/env python
'''datasetRegressor.py

Runs regression on a given dataset.
Dataset are read as Parquet file. The dataset must contain
a feature vector named "features" and a prediction column.
The column name of the prediction column must be specified
on the command lines.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.1"
__status__ = "Done"

from mmtfPyspark.ml import SparkRegressor, datasetBalancer
from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor, GeneralizedLinearRegression, LinearRegression
import sys
import time

def main(argv):

    # Name of prediction column
    label = argv[1]

    start = time.time()

    spark = SparkSession.builder \
                        .master("local[*]") \
                        .appName("datasetRegressor") \
                        .getOrCreate()

    data = spark.read.parquet(argv[0]).cache()

    vector = data.first()
    print(vector)
    featureCount = len(vector)
    print("Feature count    : {featureCount}")

    print("Dataset size (unbalanced)    : {data.count()}")

    testFraction = 0.3
    seed = 123

    # Linear Regression
    lr = LinearRegression().setLabelCol(label) \
                           .setFeaturesCol("features")
    reg = SparkRegressor(lr, label, testFraction, seed)
    matrics = reg.fit(data)
    for k,v in matrics.items(): print(f"{k}\t{v}")

    # GBTRegressor
    gbt = GBTRegressor().setLabelCol(label) \
                        .setFeaturesCol("features")
    reg = SparkRegressor(gbt, label, testFraction, seed)
    matrics = reg.fit(data)
    for k,v in matrics.items(): print(f"{k}\t{v}")

    # GeneralizedLinearRegression
    glr = GeneralizedLinearRegression().setLabelCol(label) \
                                       .setFeaturesCol("features") \
                                       .setFamily("gaussian") \
                                       .setLink("identity") \
                                       .setMaxIter(10) \
                                       .setRegParam(0.3)
    reg = SparkRegressor(glr, label, testFraction, seed)
    matrics = reg.fit(data)
    for k,v in matrics.items(): print(f"{k}\t{v}")

    end = time.time()
    print("Time: %f  sec." %(end-start))


if __name__ == "__main__":

    if len(sys.argv) < 3:
        raise Exception("python datasetClassifier.py <parquet file> <prediction column name>")
        sys.exit()

    main(sys.argv[1:])
