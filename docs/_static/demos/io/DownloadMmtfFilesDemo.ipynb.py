
# coding: utf-8

# # Download Mmtf Files Demo
# 
# Example of downloading a list of PDB entries from [RCSB]("http://mmtf.rcsb.org")
# 
# ## Imports

# In[9]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[10]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("DownloadMMTFFiles")
sc = SparkContext(conf = conf)


# ## Download a list of PDB entries using MMTF web services

# In[11]:


pdbIds = ['1AQ1','1B38','1B39','1BUH']

pdb = mmtfReader.download_mmtf_files(pdbIds, sc)


# ## Count the number of entires downloaded

# In[12]:


count = pdb.count()

print(f'number of entries downloaded : {count}')


# ## Visualize Structures

# In[13]:


structures = pdb.keys().collect()
view_structure(structures, style = 'line')


# ## Terminate Spark

# In[14]:


sc.stop()

