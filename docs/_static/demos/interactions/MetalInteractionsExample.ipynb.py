
# coding: utf-8

# # Example of using PySpark to find metal interactions

# ## Imports and variables

# In[1]:


from pyspark import SparkConf, SparkContext                    
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.interactions import InteractionFilter, GroupInteractionExtractor
from mmtfPyspark.filters import ContainsLProteinChain, Resolution
from mmtfPyspark.webfilters import Pisces
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

# In[11]:


# input parameters
sequenceIdentityCutoff = 30
resolution = 2.5
minInteractions = 4
maxInteractions = 6
distanceCutoff = 3.0

# chemical component codes of metals in different oxidation states
metals = {"V","CR","MN","MN3","FE","FE2","CO","3CO","NI","3NI", "CU","CU1","CU3","ZN","MO","4MO","6MO"}


# ## Read PDB and create PISCES non-redundant set

# In[12]:


pdb = mmtfReader.read_sequence_file(path, sc)
pdb = pdb.filter(Pisces(sequenceIdentity = sequenceIdentityCutoff, resolution = resolution))         


# ## Setup criteria for metal interactions

# In[13]:


interactions_filter = InteractionFilter()
interactions_filter.set_distance_cutoff(distanceCutoff)
interactions_filter.set_min_interactions(minInteractions)
interactions_filter.set_max_interactions(maxInteractions)
interactions_filter.set_query_groups(True, metals)

#Exclude non-polar interactions
interactions_filter.set_target_elements(False, ['H','C','P'])


# ## Tabulate interactions in a Dataframe

# In[14]:


interactions = GroupInteractionExtractor().get_interactions(pdb,interactions_filter).cache()
print(f"Metal interactions: {interactions.count()}")


# ## Select interacting atoms and orientational order parameters (q4-q6)

# In[15]:


interactions = interactions.select("pdbId",                 "q4","q5","q6",                 "element0","groupNum0","chain0",                 "element1","groupNum1","chain1","distance1",                 "element2","groupNum2","chain2","distance2",                 "element3","groupNum3","chain3","distance3",                 "element4","groupNum4","chain4","distance4",                 "element5","groupNum5","chain5","distance5",                 "element6","groupNum6","chain6","distance6").cache();

# show some example interactions
ds = interactions.dropDuplicates(["pdbId"])
df = ds.toPandas() # convert to pandas dataframe to fit table in jupyter notebook cell
df.head()


# ## Count Unique interactions by metal

# In[16]:


print("Unique interactions by metal: ")
unique_ds = interactions.groupBy(['element0']).count().sort("count")
unique_ds.show()


# ## Plot histogram for unique interactions count

# In[17]:


unique_df = unique_ds.toPandas()
unique_df.plot(x='element0', y='count', kind='bar')


# ## Terminate Spark

# In[18]:


sc.stop()

