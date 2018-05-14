
# coding: utf-8

# # DSSP Demo
# 
# This demo shows how to create and query a dssp dataset.
# 
# DSSP is a database of secondary structure assigmnets for all protein entries in the Protein Data Bank (PDB).
# 
# [DSSP Website](http://swift.cmbi.ru.nl/gv/dssp/)
# 
# ## Imports

# In[9]:


from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.mappers import StructureToPolymerChains
import time


# ## Configure Spark

# In[10]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("DSSPDemo")
sc = SparkContext(conf = conf)


# ## Download single protein (1STP)

# In[11]:


pdbIds = ["1STP"]

pdb = mmtfReader.download_mmtf_files(pdbIds, sc).cache()


# ## Flatmap to polymer chains

# In[12]:


pdb = pdb.flatMap(StructureToPolymerChains())


# ## Extract Secondary Structures

# In[13]:


ds = secondaryStructureExtractor.get_dataset(pdb)

ds.printSchema()
ds.show(2, False)


# ## Terminate Spark

# In[14]:


sc.stop()

