#!/user/bin/env python
from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.utils import traverseStructureHierarchy

# Set up spark
conf = SparkConf().setMaster("local[*]").setAppName("test")
sc = SparkContext(conf=conf)

# Download Structure 1AQ1
pdb = mmtfReader.download_mmtf_files(['1AQ1'], sc) #

# Get structure
structure = pdb.collect()[0][1]
traverseStructureHierarchy.print_metadata(structure)
traverseStructureHierarchy.print_structure_data(structure)

# Stop Spark Context
sc.stop()
