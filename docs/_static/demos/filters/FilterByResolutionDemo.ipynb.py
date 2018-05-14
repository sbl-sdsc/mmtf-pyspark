
# coding: utf-8

# # Filter By Resolution Demo
# 
# Example of reading an MMTF Hadoop Sequence file, filtering the entires by resolution, and counting the number of entires.
# 
# [Understand PDB data resolution](http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/resolution)
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import Resolution
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterByResolution")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by resolution
# 
# Entires without resolution values (eg. NMR structures) will be filtered out

# In[4]:


pdb = pdb.filter(Resolution(0.0,2.0))


# ## Count number of entires

# In[5]:


count = pdb.count()

print(f"Number of structures  : {count}")


# ## Visualize Structures

# In[6]:


structure_names = pdb.keys().collect()
view_structure(structure_names)


# ## Terminate Spark 

# In[7]:


sc.stop()

