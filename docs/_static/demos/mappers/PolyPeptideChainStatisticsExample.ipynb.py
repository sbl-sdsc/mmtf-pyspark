
# coding: utf-8

# # Poly-peptide Chain Statistics Example
# 
# Example demonstrating how to extract protein cahins from PDB entries. This example uses a flatMap function to transform a structure to its polymer chains.
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.filters import PolymerComposition
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.mappers import StructureToPolymerChains


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("polypeptideCahinStats")
sc = SparkContext(conf = conf)


# ## Read in mmtf files, flatMap to polymer chains, filter by polymer composition, and get number of groups

# In[4]:


path = "../../resources/mmtf_full_sample/"

chainLengths = mmtfReader.read_sequence_file(path, sc)                          .flatMap(StructureToPolymerChains(False, True))                          .filter(PolymerComposition(PolymerComposition.AMINO_ACIDS_20))                          .map(lambda t: t[1].num_groups)                          .cache()


# ## Print out poly-peptide chain statistics

# In[5]:


print(f"Total number of chains: {chainLengths.count()}")
print(f"Total number of groups: {chainLengths.sum()}")
print(f"Min chain length: {chainLengths.min()}")
print(f"Mean chain length: {chainLengths.mean()}")
print(f"Max chain length: {chainLengths.max()}")


# ## Terminate Spark

# In[6]:


sc.stop()

