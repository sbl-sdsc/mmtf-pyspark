
# coding: utf-8

# # Filter Exclusively By L Protein Demo
# 
# Simple example of reading an MMTF Hadoop Sequence file, filtering the entries exclusively by LProtein, and counting the number of entries. This example shows how methods can be chained for a more concise syntax.
# 
# ## Imports

# In[2]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[3]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterExclusivelyByLProtein")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files, filter by L protein, and count the entries

# In[4]:


path =  "../../resources/mmtf_reduced_sample/"

structures = mmtfReader.read_sequence_file(path, sc)                 .filter(ContainsLProteinChain(exclusive = True))

print(f"Number of L-Proteins: {structures.count()}")


# ## Visualize Structures

# In[5]:


structure_names = structures.keys().collect()
view_structure(structure_names, style='sphere')


# ## Terminate Spark 

# In[6]:


sc.stop()

