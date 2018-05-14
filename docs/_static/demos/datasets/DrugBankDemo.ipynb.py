
# coding: utf-8

# # Drug Bank Demo
# 
# ![DrugBank](./figures/drugbank.jpg)
# 
# This demo demonstrates how to access the open DrugBank dataset. This dataset contains identifiers and names for integration with other data resources.
# 
# ## Reference
#  
# Wishart DS, et al., DrugBank 5.0: a major update to the DrugBank database for 2018. Nucleic Acids Res. 2017 Nov 8.
# 
# <a href="https://dx.doi.org/10.1093/nar/gkx1037">doi:10.1093/nar/gkx1037</a>.
# 
# ## Imports

# In[2]:


from pyspark.sql import SparkSession
from mmtfPyspark.datasets import drugBankDataset


# ## Configure Spark Session

# In[4]:


spark = SparkSession.builder                    .master("local[*]")                    .appName("DrugBankDemo")                     .getOrCreate()


# ## Download open DrugBank dataset

# In[8]:


openDrugLinks = drugBankDataset.get_open_drug_links()

openDrugLinks.columns


# ## Find all drugs with an InChIKey

# In[9]:


openDrugLinks = openDrugLinks.filter("StandardInChIKey IS NOT NULL")


# ## Show some sample data

# In[10]:


openDrugLinks.select("DrugBankID","Commonname","CAS","StandardInChIKey").show()


# ## Download DrugBank dataset for approved drugs
# 
# The DrugBank password protected datasets contain more information.
# YOu need to create a DrugBank account and supply username/passwork to access these datasets.
# 
# [Create DrugBank account](https://www.drugbank.ca/public_users/sign_up)

# In[13]:


username = "<your DrugBank account username>"
password = "<your DrugBank account password>"
drugLinks = drugBankDataset.get_drug_links("APPROVED", username,password)


# ## Show some sample data from DrugLinks

# In[21]:


drugLinks.select("DrugBankID","Name","CASNumber","Formula","PubChemCompoundID",                 "PubChemSubstanceID","ChEBIID","ChemSpiderID").show()


# ## Terminate Spark

# In[7]:


sc.stop()

