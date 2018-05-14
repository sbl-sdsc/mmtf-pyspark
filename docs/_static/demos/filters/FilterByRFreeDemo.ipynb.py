
# coding: utf-8

# # Filter By RFree Demo
# 
# Example of reading an MMTF Hadoop Sequence file, filtering the entries by resolution, and counting the number of entries. This example shows how methods can be chained together.
# 
# [R Free](http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/r-value-and-r-free)
# 
# ## Imports

# In[2]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import RFree
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[3]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterByRFreeDate")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files, filter by RFree and count

# In[4]:


path = "../../resources/mmtf_reduced_sample/"

structures = mmtfReader.read_sequence_file(path, sc)                        .filter(RFree(0.15,0.25))
        
print(f"Number of structures : {structures.count()}")


# ## Visualize Structures

# In[6]:


structure_names = structures.keys().collect()
view_structure(structure_names, style='stick')


# ## Terminate Spark 

# In[7]:


sc.stop()

