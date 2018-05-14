
# coding: utf-8

# # Write MMTF Subset Demo
# 
# Simple example writting a subset of mmtf files
# 
# ## Imports

# In[2]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader, mmtfWriter
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[3]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("WriteMMTFSubset")
sc = SparkContext(conf = conf)


# ## Read in a fractions of entries from a local Hadoop Sequence File

# In[4]:


path = "../../resources/mmtf_full_sample/"
fraction= 0.5
seed = 123

pdb = mmtfReader.read_sequence_file(path, sc, fraction = fraction, seed = seed)

count = pdb.count()

print(f'number of pdb entries read : {count}')


# ## Visualize Structures

# In[5]:


structures = pdb.keys().collect()
view_structure(structures, style='stick')


# ## Save this subset in a Hadoop Sequence File

# In[4]:


write_path = "./mmtf_subset"

mmtfWriter.write_sequence_file(write_path, sc, pdb)


# ## Terminate Spark

# In[5]:


sc.stop()

