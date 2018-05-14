
# coding: utf-8

# # Secondary Structure Segment Demo
# 
# This demo shows how to get a dataset of secondary structure segment
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.datasets import secondaryStructureSegmentExtractor


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("secondaryStructureSegmentDemo")
sc = SparkContext(conf = conf)


# ## Download protein (1STP)
# 
# ### Note: Need to use SparkContext as parameter to download Mmtf files

# In[3]:


pdb = mmtfReader.download_mmtf_files(['1STP'], sc).cache()


# ## Map protein to polymer chains and apply LProteinChain filter

# In[4]:


pdb = pdb.flatMap(StructureToPolymerChains())          .filter(ContainsLProteinChain())


# ## Extract secondary structure element 'E'

# In[5]:


segmentLength = 25
ds = secondaryStructureSegmentExtractor.get_dataset(pdb, segmentLength)

ds.show(50, False)


# ## Terminate Spark

# In[6]:


sc.stop()

