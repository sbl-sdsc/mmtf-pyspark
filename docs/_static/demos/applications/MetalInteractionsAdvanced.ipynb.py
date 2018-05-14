
# coding: utf-8

# # Example Of Using PySpark To Find Metal Interactions 
# 
# <br>
# <img src="./figures/metal_interaction.png" width=600, align="left">
# <br>

# ## Imports and variables

# In[1]:


from pyspark import SparkConf, SparkContext                    
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.interactions import InteractionFilter, GroupInteractionExtractor
from mmtfPyspark.filters import ContainsLProteinChain, Resolution
from mmtfPyspark.webfilters import Pisces
from mmtfPyspark.structureViewer import group_interaction_viewer, metal_distance_widget
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
import py3Dmol
import time
                                                               
# Create variables                                             
APP_NAME = "MMTF_Spark"                                        
path = "../../resources/mmtf_full_sample/"

# Configure Spark                                              
conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")  
sc = SparkContext(conf=conf)                                   


# ## Read PDB and create PISCES non-redundant set

# In[2]:


pdb = mmtfReader.read_sequence_file(path, sc)
pdb = pdb.filter(Pisces(sequenceIdentity = 30, resolution = 2.5))         


# ## Setup criteria for metal interactions

# In[3]:


# Chemical component codes of metals in different oxidation states
metals = {"V","CR","MN","MN3","FE","FE2","CO","3CO","NI","3NI", "CU","CU1","CU3","ZN","MO","4MO","6MO"}

interactions_filter = InteractionFilter(distanceCutoff = 3.0, minInteractions=4, maxInteractions=6)
interactions_filter.set_query_groups(True, metals)

# Exclude non-polar interactions
interactions_filter.set_target_elements(False, ['H','C','P'])


# ## Tabulate interactions in a Dataframe

# In[4]:


interactions = GroupInteractionExtractor().get_interactions(pdb,interactions_filter).cache()
print(f"Metal interactions: {interactions.count()}")


# ## Select interacting atoms and orientational order parameters (q4-q6)

# In[5]:


interactions = interactions.select("pdbId",                 "q4","q5","q6",                 "element0","groupNum0","chain0",                 "element1","groupNum1","chain1","distance1",                 "element2","groupNum2","chain2","distance2",                 "element3","groupNum3","chain3","distance3",                 "element4","groupNum4","chain4","distance4",                 "element5","groupNum5","chain5","distance5",                 "element6","groupNum6","chain6","distance6").cache();

# show some example interactions
ds = interactions.dropDuplicates(["pdbId"])
df = ds.toPandas() # convert to pandas dataframe to fit table in jupyter notebook cell
df.head()


# ## Count Unique interactions by metal

# In[6]:


print("Unique interactions by metal: ")
unique_ds = interactions.groupBy(['element0']).count().sort("count")
unique_ds.show()


# ## Violin plot using Seaborn

# In[7]:


# tranform Dataset to pandas DataFrame
df = interactions.toPandas()

# Set fonts
sns.set(font_scale=2)

# Make subplots
fig, ax = plt.subplots(1,3, sharey = True, figsize = (30,5))

# Loop through subplots
for i in range(3):
    subplot = sns.violinplot(x="element0", y=f"q{i+4}", palette="muted", data=df, ax = ax[i])
    subplot.set(xlabel="Metals", ylabel="", title=f"q{i+4}")


# ## Make Violin plots for Metal-Elements distances

# In[8]:


# Create dataframe subsets for elements 1-6
df_sub = [df[['element0', f'element{i}', f'distance{i}']]          .rename(columns={'element0':'Metal', f'element{i}':'Element', f'distance{i}':'Distance'})           for i in range(1,7)]

# Vertically concating the dataframe subsets
df_concat = pd.concat(df_sub)

# Drop rows with NaN values
df_concat.dropna(inplace = True)
                  
metal_distance_widget(df_concat)


# ## Create subset based on Metal and q values

# In[9]:


df_sub = df[df["element0"] == 'Zn']    # Fitler only Zinc interactinos 
df_sub = df_sub.sort_values(["q4"], ascending = False).dropna(subset=['q4'])    #Sort by q4 values and drop NaN
df_sub = df_sub[df_sub['q5'] != np.nan]    # Revove interactions where q5 has values

df_sub.head(10)


# ## Visualize Data

# In[10]:


group_interaction_viewer(df_sub, 'q4')


# ## Terminate Spark

# In[11]:


sc.stop()

