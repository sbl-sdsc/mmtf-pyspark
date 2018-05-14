
# coding: utf-8

# # Swiss Model Dataset
# 
# This demo shows how to access metadata for SWISS-MODEL homology models.
# 
# ## Reference
#  
# Bienert S, Waterhouse A, de Beer TA, Tauriello G, Studer G, Bordoli L, Schwede T (2017). The SWISS-MODEL Repository - new features and functionality, Nucleic Acids Res. 45(D1):D313-D319.
#     * https://dx.doi.org/10.1093/nar/gkw1132
#    
# Biasini M, Bienert S, Waterhouse A, Arnold K, Studer G, Schmidt T, Kiefer F, Gallo Cassarino T, Bertoni M, Bordoli L, Schwede T(2014). The SWISS-MODEL Repository - modelling protein tertiary and quaternary structure using evolutionary information, Nucleic Acids Res. 42(W1):W252–W258.
#     * https://doi.org/10.1093/nar/gku340
# 
# ## Imports

# In[1]:


from pyspark.sql import SparkSession
from mmtfPyspark.datasets import swissModelDataset


# ## Configure Spark Session

# In[2]:


spark = SparkSession.builder                    .master("local[*]")                    .appName("SwissModelDatasetDemo")                     .getOrCreate()


# ## Download metadata for Swiss-Model homology

# In[3]:


# list of uniProtIds to be retrived from Swiss-Model
uniProtIds = ['P36575','P24539','O00244']

ds = swissModelDataset.get_swiss_models(uniProtIds)


# ## Show results

# In[5]:


df = ds.toPandas()
df.head()


# ## Terminate Spark

# In[7]:


sc.stop()

