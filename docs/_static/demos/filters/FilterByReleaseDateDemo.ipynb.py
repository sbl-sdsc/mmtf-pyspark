
# coding: utf-8

# # Filter By Release Date Demo
# 
# This example demonstrates how to filter structure with specified release date range
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ReleaseDate
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterByReleaseDate")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files, filter and count

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

structures = mmtfReader.read_sequence_file(path, sc)                 .filter(ReleaseDate("2000-01-28","2017-02-28"))
        
print(f"Number of structure released between 2000-01-28 and 2017-02-28 is: {structures.count()}")


# ## Visualize Structures

# In[4]:


structure_names = structures.keys().collect()
view_structure(structure_names, style='line')


# ## Terminate Spark 

# In[5]:


sc.stop()

