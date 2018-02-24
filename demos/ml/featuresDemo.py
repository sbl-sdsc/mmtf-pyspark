#!/usr/bin/env python
'''

featuresDemo.py

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import col, when
from mmtfPyspark.ml import proteinSequenceEncoder
from mmtfPyspark.mappers import structureToPolymerChains
from mmtfPyspark.filters import containsLProteinChain
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.webfilters import pisces
from mmtfPyspark.io import MmtfReader
import time


def main():
    '''
    Demo to create a feature vector for protein fold classification.
    In this demo we try to classify a protein chain as either an
    all alpha or all beta protein based on protein sequence. We use
    n-grams and Word2Vec representation of the protein sequence as a
    feature vector.
    '''

    start = time.time()

    conf = SparkConf() \
            .setMaster("local[*]") \
            .setAppName("featuresDemo")
    sc = SparkContext(conf = conf)
    path = "../../resources/mmtf_reduced_sample/"

    # Read MMTF Hadoop sequence file and create a non-redundant set (<=40% seq. identity)
    # of L-protein chains
    sequenceIdentity = 40
    resolution = 2.0

    pdb = MmtfReader \
            .read_sequence_file(path, sc) \
            .filter(pisces(sequenceIdentity, resolution)) \
            .flatMap(structureToPolymerChains()) \
            .filter(pisces(sequenceIdentity, resolution)) \
            .filter(containsLProteinChain()) \

    # Get secondary structure content
    data = secondaryStructureExtractor.getDataset(pdb)

    # classify chains by secondary structure type
    minThreshold = 0.05
    maxThreshold = 0.15
    data = addProteinFoldType(data, minThreshold, maxThreshold)


    # add Word2Vec encoded feature vector
    encoder = proteinSequenceEncoder(data)
    n = 2 # Create 2-grams
    windowSize = 25 # 25-amino residue window size for Word2Vec
    vectorSize = 50 # dimension of feature vector
    data = encoder.overlappingNgramWord2VecEncode(n, windowSize, vectorSize).cache()

    data.printSchema()
    data.show(25)

    # keep only a subset of relevant fields for futher processing
    data = data.select("structureChainId", "alpha", "beta", "coil", "foldType", "features")

    data.write.mode("overwrite").format("parquet").save("/home/marshuang80/PDB/data/demo.parquet")

    end = time.time()

    print("Time: %f  sec." %(end-start))

    sc.stop()


def addProteinFoldType(data, minThreshold, maxThreshold):
    '''
    Adds a column "foldType" with three major secondary structure class:
    "alpha", "beta", "alpha+beta", and "other" based upon the fraction of alpha/beta content.

    The simplified syntax used in this method relies on two imports:
        from pyspark.sql.functions import when
        from pyspark.sql.functions import col

    Attributes:
        data (Dataset<Row>): input dataset with alpha, beta composition
        minThreshold (float): below this threshold, the secondary structure is ignored
        maxThreshold (float): above this threshold, the secondary structure is ignored
    '''

    return data.withColumn("foldType", \
                           when((col("alpha") > maxThreshold) & (col("beta") < minThreshold), "alpha"). \
                           when((col("beta") > maxThreshold) & (col("alpha") < minThreshold), "beta"). \
                           when((col("alpha") > maxThreshold) & (col("beta") < minThreshold), "alpha+beta"). \
                           otherwise("other")\
                           )


if __name__ == "__main__":
    main()
