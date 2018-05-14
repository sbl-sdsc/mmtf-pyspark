
# coding: utf-8

# # Filter By Groups Demo
# 
# This example demonstrates how to filter structures with specified groups (residues). Groups are specified by their one, two or three letter codes e.g. "F", "MG", "ATP".
# 
# For full list, please refer to [PDB Chemical Component Dictionary](https://www.wwpdb.org/data/ccd)
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ContainsGroup
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterByGroupsDate")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by groups and count

# In[5]:


filtered_structures = pdb.filter(ContainsGroup("ATP","MG"))

print(f"Number of structure with ATP + MG : {filtered_structures.count()}")


# ## Visualize 3D structures of filtered structures

# In[12]:


structure_names = filtered_structures.keys().collect()
view_structure(structure_names, style='stick')


# ## Terminate Spark 

# In[13]:


sc.stop()

