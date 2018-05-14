
# coding: utf-8

# # Example of using PySpark to find polymer interaction fingerprint
# 
# Demo how to calculate polymer interaction data and maps it to polymer chains.

# ## Imports and variables

# In[1]:


from pyspark import SparkConf, SparkContext                    
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.interactions import InteractionFilter, InteractionFingerprinter
                                                               
# Create variables                                             
APP_NAME = "MMTF_Spark"                                        

# Configure Spark                                              
conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")  
sc = SparkContext(conf=conf)                                   


# ## Download 1OHR structure

# In[2]:


pdb = mmtfReader.download_mmtf_files(['1OHR'], sc)


# ## Find ASP-ARG salt bridges

# In[3]:


interactionFilter = InteractionFilter(distanceCutoff=3.5, minInteractions=1)
interactionFilter.set_query_groups(True, "ASP")
interactionFilter.set_query_atom_names(True, ['OD1','OD2'])
interactionFilter.set_target_groups(True, "ARG")
interactionFilter.set_target_atom_names(True, ['NH1','NH2'])

interactions = InteractionFingerprinter.get_polymer_interactions(pdb, interactionFilter)
interactions.toPandas().head(10)    


# ## Terminate Spark

# In[ ]:


sc.stop()

