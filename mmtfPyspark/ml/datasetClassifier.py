#!/usr/bin/env python
'''

datasetClassifier.py

Runs binary and multi-class classifiers on a given dataset.
Dataset are read as Parquet file. The dataset must contain
a feature vector named "features" and a classification column.
The column name of the classification column must be specified
on the command lines.

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Debug"
'''

from mmtfPyspark.ml import sparkMultiClassClassifier, datasetBalancer
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression, MultilayerPerceptronClassifier, RandomForestClassifier
import sys
import time

def main(argv):

    # Name of prediction column
    label = argv[1]

    start = time.time()

    spark = SparkSession.builder \
                        .master("local[*]") \
                        .appName("datasetClassifier") \
                        .getOrCreate()

    data = spark.read.parquet(argv[0]).cache()

    vector = data.first()["features"]
    featureCount = len(vector)

    print(f"Feature count    : {featureCount}")
    classCount = int(data.select(label).distinct().count())
    print(f"Class count    : {classCount}")
    print(f"Dataset size (unbalanced)    : {data.count()}")
    data.groupby(label).count().show(classCount)

    data = datasetBalancer.downsample(data, label, 1)

    print(f"Dataset size (balanced)  : {data.count()}")
    data.groupby(label).count().show(classCount)

    testFraction = 0.3
    seed = 123

    # DecisionTree
    dtc = DecisionTreeClassifier()
    mcc = sparkMultiClassClassifier(dtc, label, testFraction, seed)
    matrics = mcc.fit(data)
    for k,v in matrics.items(): print(f"{k}\t{v}")

    # RandomForest
    rfc = RandomForestClassifier()
    mcc = sparkMultiClassClassifier(rfc, label, testFraction, seed)
    matrics = mcc.fit(data)
    for k,v in matrics.items(): print(f"{k}\t{v}")

    # LogisticRegression
    lr = LogisticRegression()
    mcc = sparkMultiClassClassifier(lr, label, testFraction, seed)
    matrics = mcc.fit(data)
    for k,v in matrics.items(): print(f"{k}\t{v}")

    # MultilayerPerceptronClassifier
    layers = [featureCount, 10, classCount]
    mpc = MultilayerPerceptronClassifier().setLayers(layers) \
                                          .setBlockSize(128) \
                                          .setSeed(1234) \
                                          .setMaxIter(200)
    mcc = sparkMultiClassClassifier(mpc, label, testFraction, seed)
    matrics = mcc.fit(data)
    for k,v in matrics.items(): print(f"{k}\t{v}")

    end = time.time()
    print("Time: %f  sec." %(end-start))


if __name__ == "__main__":

    if len(sys.argv) < 3:
        raise Exception("python datasetClassifier.py <parquet file> <prediction column name>")
        sys.exit()

    main(sys.argv[1:])
