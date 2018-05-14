
# coding: utf-8

# # Secondary Structure Element Demo
# 
# This demo shows how to get a dataset of secondary structure elements
# 
# ## Imports

# In[10]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.datasets import secondaryStructureElementExtractor


# ## Configure Spark

# In[11]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("secondaryStructureElementDemo")
sc = SparkContext(conf = conf)


# ## Download protein (1STP)
# 
# ### Note: Need to use SparkContext as parameter to download Mmtf files

# In[12]:


pdb = mmtfReader.download_mmtf_files(['1STP'], sc).cache()


# ## Map protein to polymer chains and apply LProteinChain filter

# In[13]:


pdb = pdb.flatMap(StructureToPolymerChains())          .filter(ContainsLProteinChain())


# ## Extract secondary structure element 'E'

# In[14]:


ds = secondaryStructureElementExtractor.get_dataset(pdb, 'E', 6)

ds.show(50, False)


# ## Terminate Spark

# In[15]:


sc.stop()

