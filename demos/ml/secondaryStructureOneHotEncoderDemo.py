#!/usr/bin/env python
'''
SecondaryStructureOneHotEncoderDemo.py:

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Debug"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.ml import proteinSequenceEncoder
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.datasets import secondaryStructureSegmentExtractor
from mmtfPyspark.webfilters import pisces
from mmtfPyspark.io import MmtfReader
import time

# TODO data count is more than Java

def main():
    '''This class creates a dataset of sequence segments dericed from a
    non-redundant set. The dataset contains the sequence segment, the DSSP Q8
    and DSSP Q3 code of the center residue in a seuqnce segment, and a one-hot
    encoding of the sequence segment.
    The dataset is saved in a file specified by the user
    '''

    start = time.time()

    conf = SparkConf() \
            .setMaster("local[*]") \
            .setAppName("SecondaryStructureOneHotEncoderDemo")
    sc = SparkContext(conf = conf)

    # Read MMTF Hadoop sequence file and create a non-redundant set
    # (<=20% seq. identity) of L-protein chains

    path = "../../resources/mmtf_reduced_sample/"

    sequenceIdentity = 20
    resolution = 2.0
    fraction = 0.1
    seed = 123

    #.download_mmtf_files(["2ONX",'1JLP','5X6H','5L2G','2MK1' ],sc) \

    pdb = MmtfReader \
            .read_sequence_file(path, sc) \
            .flatMap(StructureToPolymerChains()) \
            .filter(pisces(sequenceIdentity, resolution)) \
            .filter(ContainsLProteinChain()) \
            .sample(False, fraction, seed)

    segmentLength = 11
    data = secondaryStructureSegmentExtractor.getDataset(pdb, segmentLength).cache()
    print(f"original data   : {data.count()}")

    data = data.dropDuplicates(["labelQ3", "sequence"]).cache()
    print(f"- duplicate Q3/seq  : {data.count()}")

    data = data.dropDuplicates(["sequence"])
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
