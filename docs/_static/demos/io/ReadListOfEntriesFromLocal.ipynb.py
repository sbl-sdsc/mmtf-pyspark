
# coding: utf-8

# # Read List Of Entries From Local Hadoop Sequence File
# 
# Simple example reading a list of PDB entries from local Hadoop Sequence Files
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("ReadListFromLocal")
sc = SparkContext(conf = conf)


# ## Read in local Hadoop Sequence Files and count number of entries

# In[3]:


path = "../../resources/mmtf_full_sample/"
pdbIds = ["1M4L", "1LXJ", "4XPX"]

pdb = mmtfReader.read_sequence_file(path, sc, pdbId = pdbIds,)

count = pdb.count()

print(f'number of pdb entries read : {count}')


# ## Visualize Structures

# In[4]:


structures = pdb.keys().collect()
view_structure(structures)


# ## Terminate Spark

# In[5]:


sc.stop()

