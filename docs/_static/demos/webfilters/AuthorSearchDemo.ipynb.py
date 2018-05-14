
# coding: utf-8

# # Author Search Demo
# 
# ![pdbj](https://pdbj.org/content/default.svg)
# 
# AuthorSearch shows how to query PDB structures by metadata. This example queries the name fields in the audit_author and citation_author categories.
# 
# 
# ## References
# 
# Each category represents a table and fields represent database columns, see:
# [Available tables and columns](https://pdbj.org/mine-rdb-docs)
# 
# Data are provided through: 
# [Mine 2 SQL](https://pdbj.org/help/mine2-sql)
# 
# Queries can be designed with the interactive
# [PDBj Mine 2 query service](https://pdbj.org/mine/sql)
# 
# 
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.webfilters import PdbjMineSearch
from mmtfPyspark.io import mmtfReader


# ## Configure Spark Context

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("AuthorSearchDemo")
sc = SparkContext(conf = conf)


# ## Query to find PDB structures for Doudna, J.A. as a deposition (audit) author or as an author in the primary PDB citation

# In[6]:


sqlQuery = "SELECT pdbid from audit_author "                 + "WHERE name LIKE 'Doudna%J.A.%' "                 + "UNION "                 + "SELECT pdbid from citation_author "                 + "WHERE citation_id = 'primary' AND name LIKE 'Doudna%J.A.%'"


# ## Read PDB and filter by author

# In[8]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)                 .filter(PdbjMineSearch(sqlQuery))

print(f"Number of entries matching query: {pdb.count()}")


# ## Terminate Spark Context

# In[9]:


sc.stop()

