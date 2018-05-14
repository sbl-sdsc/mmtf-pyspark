
# coding: utf-8

# # Simple Query Demo
# 
# ![pdbj](https://pdbj.org/content/default.svg)
# 
# PDBj Mine 2 RDB keyword search query and MMTF filtering using pdbid.
# 
# [PDBj Mine Search Website](https://pdbj.org/mine)
# 
# ## Imports

# In[8]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.webfilters import PdbjMineSearch
from mmtfPyspark.datasets import pdbjMineDataset
from mmtfPyspark.io import mmtfReader


# ## Configure Spark Context

# In[9]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("SimpleQuerySearch")
    
sc = SparkContext(conf = conf)


# ## Read in MMTF files from local directory

# In[10]:


path = "../../resources/mmtf_full_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Apply a SQL search on PDBj using a filter
# 
# Very simple query; this gets the pdbids for all entries modified since 2016-06-28 with a resulution better than 1.5 A

# In[11]:


sql = "select pdbid from brief_summary where modification_date >= '2016-06-28' and resolution < 1.5"

search = PdbjMineSearch(sql)
count = pdb.filter(search).keys().count()
print(f"Number of entries using sql to filter: {count}")


# ## Apply a SQL search on PDBj and get a dataset

# In[12]:


dataset = pdbjMineDataset.get_dataset(sql)
dataset.show(5)


# ## Terminate Spark Context

# In[13]:


sc.stop()

