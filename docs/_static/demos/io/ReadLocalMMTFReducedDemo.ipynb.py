
# coding: utf-8

# # Read Local MMTF Full Demo
# 
# Simple example reading PDB entries from local Hadoop Sequence Files
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("ReadLocalMMTFReduced")
sc = SparkContext(conf = conf)


# ## Read in local Hadoop Sequence Files and count number of entries

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)

count = pdb.count()

print(f'number of pdb entries read : {count}')


# In[ ]:


## Visualize Structures


# In[5]:


structures = pdb.keys().collect()
view_structure(structures, style = 'sphere')


# ## Terminate Spark

# In[6]:


sc.stop()

