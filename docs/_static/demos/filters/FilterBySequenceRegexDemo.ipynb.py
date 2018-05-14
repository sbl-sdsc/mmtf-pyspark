
# coding: utf-8

# # Filter By Sequence Regex Demo
# 
# This example shows how to filter poteins by their sequence regualar expression. 
# 
# [More about regular expression](https://www.regular-expressions.info)
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ContainsSequenceRegex
from mmtfPyspark.structureViewer import view_group_interaction


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterBySequenceRegex")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by sequence regular expression
# 
# #### Zinc Finger Motif  regular expression:    C.{2,4}C.{12}H.{3,5}H
# 
# <img src="./figures/ZincFingerMotif.png" style="width: 300px;"/>

# In[8]:


structures = pdb.filter(ContainsSequenceRegex("C.{2,4}C.{12}H.{3,5}H"))


# ## Count number of entires

# In[9]:


count = structures.count()

print(f"Number of entries containing Zinc figure motif is : {count}")


# ## Visualize Structure Zinc interactions

# In[12]:


structure_names = structures.keys().collect()
view_group_interaction(structure_names, 'ZN', style='line')


# ## Terminate Spark 

# In[13]:


sc.stop()

