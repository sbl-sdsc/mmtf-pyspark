#!/usr/bin/env python
'''

secondaryStructureShiftedWord2VecEncoder.py

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Debug"
'''

from pyspark import SparkConf, SparkContext, SQLContext
from src.main.ml import proteinSequenceEncoder
from src.main.mappers import structureToPolymerChains
from src.main.filters import containsLProteinChain
from src.main.datasets import secondaryStructureSegmentExtractor
from src.main.rcsbfilters import pisces
from src.main.io import MmtfReader
import time

# TODO data count is more than Java

def main():
    '''
    This class creates a dataset of sequence segment derived from a
    non-redundant set. The dataset contains the sequence segment, the DSSP
    Q8 and DSSP Q3 code of the center residue in a sequence segment, and a
    Word2Vec encoding of the sequence segment.
    The Data is saved in JSON file specified by the user.
    '''

    start = time.time()

    conf = SparkConf() \
            .setMaster("local[*]") \
            .setAppName("secondaryStructureWord2VecEncodeDemo")
    sc = SparkContext(conf = conf)

    # Read MMTF Hadoop sequence file and create a non-redundant set
    # (<=20% seq. identity) of L-protein chains

    path = "/home/marshuang80/PDB/reduced"

    sequenceIdentity = 20
    resolution = 2.0
    fraction = 0.1
    seed = 123

    pdb = MmtfReader \
            .readSequenceFile(path, sc) \
            .flatMap(structureToPolymerChains()) \
            .filter(pisces(sequenceIdentity, resolution)) \
            .filter(containsLProteinChain()) \
            .sample(False, fraction, seed)

    segmentLength = 25
    data = secondaryStructureSegmentExtractor.getDataset(pdb, segmentLength).cache()

    # add Word2Vec encoded feature vector
    encoder = proteinSequenceEncoder(data)
    windowSize = (segmentLength -1) // 2
    vectorSize = 50
    data = encoder.shifted3GramWord2VecEncode(windowSize, vectorSize).cache()

    data.printSchema()
    data.show(25, False)

    end = time.time()

    print("Time: %f  sec." %(end-start))

    sc.stop()

if __name__ == "__main__":
    main()
