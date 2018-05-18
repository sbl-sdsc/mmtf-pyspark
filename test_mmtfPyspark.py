#!/user/bin/env python
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.utils import traverseStructureHierarchy
from pyspark.sql import SparkSession
# Set up spark
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# Download Structure 1AQ1
pdb = mmtfReader.download_mmtf_files(['1AQ1']) #

# Get structure
structure = pdb.collect()[0][1]
traverseStructureHierarchy.print_metadata(structure)
traverseStructureHierarchy.print_structure_data(structure)

# Stop Spark Context
spark.stop()
