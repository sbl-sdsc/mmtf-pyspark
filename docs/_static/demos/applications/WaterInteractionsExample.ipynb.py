
# coding: utf-8

# # Example of using mmtfPyspark to find water interactions
# 

# ## Imports and variables

# In[1]:


from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.interactions import InteractionFilter, GroupInteractionExtractor, ExcludedLigandSets
from mmtfPyspark.filters import ContainsLProteinChain, Resolution
import matplotlib.pyplot as plt
import pandas as pd
import py3Dmol
import time

                                                               
# Create variables                                             
APP_NAME = "MMTF_Spark"                                        
path = "../../resources/mmtf_full_sample/"

# Configure Spark                                              
conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]") 
sc = SparkContext(conf=conf)                                   


# ## Define Variables

# In[2]:


# input parameters
resolution = 2.0
minInteractions = 2
maxInteractions = 4
distanceCutoff = 3.0
bFactorCutoff = 1.645
includeWaters = True


# ## Read PDB and filter by resolution and only include proteins

# In[3]:


pdb = mmtfReader.read_sequence_file(path, sc)
pdb = pdb.filter(Resolution(minResolution=0.0, maxResolution=2.0))         .filter(ContainsLProteinChain(exclusive=True))


# ## Setup criteria for metal interactions

# In[4]:


interactions_filter = InteractionFilter()
interactions_filter.set_distance_cutoff(3.0)
interactions_filter.set_normalized_b_factor_cutoff(1.645)
interactions_filter.set_min_interactions(2)
interactions_filter.set_max_interactions(4)
interactions_filter.set_query_groups(True, ["HOH"])
interactions_filter.set_query_elements(True, "O")    # Only use water oxygen
interactions_filter.set_target_elements(True, ["O", "N", "S"])


# ## Exclude "uninteresting" ligands 

# In[5]:


prohibitedGroups = ExcludedLigandSets.ALL_GROUPS
if not includeWaters:
    prohibitedGroups.add("HOH")
interactions_filter.set_prohibited_target_groups(prohibitedGroups)


# ## Calculate interactions

# In[6]:


data = GroupInteractionExtractor().get_interactions(structures=pdb, interactionFilter=interactions_filter)


# ## Define Filter Bridging Water Interactions Function

# In[7]:


def filter_bridging_water_interactions(data, maxInteractions):
    if maxInteractions == 2:
        data = data.filter((col("type1") == "LGO") |                            (col("type2") == "LGO"))
        data = data.filter((col("type1") == "PRO") |                            (col("type2") == "PRO"))
    elif maxInteractions == 3:
        data = data.filter((col("type1") == "LGO") |                            (col("type2") == "LGO") |                            (col("type3") == "LGO"))
        data = data.filter((col("type1") == "PRO") |                            (col("type2") == "PRO") |                            (col("type3") == "PRO"))
    elif maxInteractions == 4:
        data = data.filter((col("type1") == "LGO") |                            (col("type2") == "LGO") |                            (col("type3") == "LGO") |                            (col("type4") == "LGO"))
        data = data.filter((col("type1") == "PRO") |                            (col("type2") == "PRO") |                            (col("type3") == "PRO") |                            (col("type4") == "PRO"))
    else:
        raise ValueError("maxInteractions > 4 are not supported yet")
    return data


# ## Keep only interactions with at least one organic ligand and one protein interaction

# In[8]:


data = filter_bridging_water_interactions(data, maxInteractions=4).cache()

print(f"Hits(all): {data.count()}")
data = data.toPandas()
data.head(50)


# ## Terminate Spark

# In[ ]:


sc.stop()

