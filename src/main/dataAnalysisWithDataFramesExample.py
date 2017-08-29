#!/usr/bin/env python
'''
dataAnalysisWithDataFramesExample.py:

Example script to demonstrate how to do data analysis with dataframes
using mmtf-pyspark

Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
'''

from pyspark import SparkConf, SparkContext
from src.main import MmtfReader
from src.main.rcsbfilters import pisces
from src.main.datasets import groupInteractionExtractor

# Create variables
APP_NAME = "MMTF_Spark"
path = "/home/marshuang80/PDB/full"

# Configure Spark
conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")
sc = SparkContext(conf=conf)

# Read all PDB entries
pdb = MmtfReader.readSequenceFile(path, sc)

# Save a non-redundant subset using Pisces filter (R. Dunbrack)
sequenceIdentity = 20
resolution = 2.0
pdb = pdb.filter(pisces(sequenceIdentity, resolution))

# Extract and list top interacting groups for Zinc in PDB
cutoffDistance = 3.0
finder = groupInteractionExtractor("ZN", cutoffDistance)
interactions = finder.getDataset(pdb)

# Show the top 10 interacting groups
interactions.filter("element2 != 'C'") \
			.groupBy("residue2") \
			.count() \
			.sort("count", ascending=False) \
			.show(10)
