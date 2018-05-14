
# coding: utf-8

# # UniProt Demo
# 
# This demo shows how to create and query a UniProt dataset.
# 
# ![UniPort](http://www.uniprot.org/images/logos/uniprot-rgb-optimized.svg)
# 
# 
# ### Binder can't fetch FTP, this filter will only work locally
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.datasets import uniProt


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("UniprotDemo")
sc = SparkContext(conf = conf)


# ## Get UniProt Datasets

# In[3]:


ds = uniProt.get_dataset(uniProt.SWISS_PROT)


# ## Display UniProt Dataset

# In[4]:


ds.show(20, False)


# ## Terminate Spark

# In[5]:


sc.stop()

