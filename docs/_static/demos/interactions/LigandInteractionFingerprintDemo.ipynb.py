
# coding: utf-8

# # Example of using PySpark to find ligand interaction fingerprint
# 
# Demo how to calculate ligand-polymer interaction data and maps it to polymer chains.

# ## Imports and variables

# In[2]:


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


# ## Find interactions of small molecules (except water)

# In[3]:


interactionFilter = InteractionFilter()
interactionFilter.set_distance_cutoff(4.0)
interactionFilter.set_query_groups(False, "HOH") # ignore water interactions

interactions = InteractionFingerprinter.get_ligand_polymer_interactions(pdb, interactionFilter)
interactions.toPandas().head(10)    


# ## Terminate Spark

# In[3]:


sc.stop()

