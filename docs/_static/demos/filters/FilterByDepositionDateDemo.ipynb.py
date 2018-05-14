
# coding: utf-8

# # Filter By Deposition Date Demo
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import DepositionDate
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("FilterByDepositionDate")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by deposition date

# In[4]:


pdb = pdb.filter(DepositionDate('1999-02-26','1999-02-28'))


# ## Count number of entires

# In[5]:


count = pdb.count()

print(f"Number of structure desposited between 1999-02-26 and 1999-02-28 is : {count}")


# ## View 3D structures

# In[6]:


pdbIds = pdb.keys().collect()

view_structure(pdbIds)


# ## Terminate Spark 

# In[7]:


sc.stop()

