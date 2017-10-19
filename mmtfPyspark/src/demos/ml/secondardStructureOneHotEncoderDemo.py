#!/usr/bin/env python
'''
SecondaryStructureOneHotEncoderDemo.py:

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from src.ml.proteinSequenceEncoder import oneHotEncode
from src.mapper.structureToPolymerChains
from src.filters.containsLProteinChain
from src.datasets.secondaryStructureSegmentExtractor
from rcsbfilters import pisces
import time


def main():
    start = time.time()

    conf = SparkConf() \
            .setMaster("local[*]") \
            .setAppName("secondaryStructureSegmentDemo")
    sc = SparkContext(conf = conf)

    # Read MMTF Hadoop sequence file and create a non-redundant set
    # (<=20% seq. identity) of L-protein chains

    sequenceIdentity = 20
    resolution = 2.0
    fraction = 0.1
    seed = 123

    pdb = MmtfReader \
            .readSequenceFile(path, sc) \
            .flatMap(structureToPolymerChains()) \
            .filter(pisces(sequenceIdentity, resolution) \
            .filter(containsLProteinChain) \
            .sample(False, fraction, seed)

    segmentLength = 11
    data = secondaryStructureSegmentExtractor.getDataset(pdb, segmentLength).cache()
    print(f"original data   : {data.count()}")

    data = data.dropDuplicates("labelQ3", "sequence").cache()
    print(f"- duplicate Q3/seq  : {data.count()}")

    data = data.dropDuplicates("sequences")
    print(f"- duplicate seq  : {data.count()}")

    encoder = proteinSequenceEncoder(data)
    data = encoder.oneHotEncode()

    data.printSchema()
    data.show(25, False)

    end = time.time()

    print("Time: %f  sec." %(end-start))

    sc.stop()

if __name__ == "__main__":
    main()
