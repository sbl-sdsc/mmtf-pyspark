
# coding: utf-8

# # Keywork Search Demo
# 
# ![pdbj](https://pdbj.org/content/default.svg)
# 
# PDBj Mine 2 RDB keyword search query and MMTF filtering using pdbid.
# This filter searches the 'keyword' column in the brief_summary table for a keyword and returns a couple of columns for the matching entries.
# 
# [PDBj Mine Search Website](https://pdbj.org/mine)
# 
# ## Imports

# In[17]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.webfilters import PdbjMineSearch
from mmtfPyspark.datasets import pdbjMineDataset
from mmtfPyspark.io import mmtfReader


# ## Configure Spark Context

# In[18]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("keywordSearch")
sc = SparkContext(conf = conf)


# ## Read in MMTF files from local directory

# In[20]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Apply a SQL search on PDBj using a filter

# In[21]:


sql = "select pdbid from keyword_search('porin')"

pdb = pdb.filter(PdbjMineSearch(sql))
print(pdb.keys().collect())
print("\n")
print(f"Number of entries matching query: {pdb.count()}")


# ## Apply a SQL search on PDBj and get a dataset

# In[14]:


sql = "select pdbid, resolution, biol_species, db_uniprot, db_pfam, hit_score from keyword_search('porin') order by hit_score desc"

dataset = pdbjMineDataset.get_dataset(sql)
dataset.show(10)


# ## Terminate Spark Context

# In[15]:


sc.stop()

