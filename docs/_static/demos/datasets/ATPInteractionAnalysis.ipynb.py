
# coding: utf-8

# # ATP Interaction Anaylsis
# 
# This demo shows how to create a dataset of ATP Interating atoms.
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.datasets import groupInteractionExtractor
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.webfilters import Pisces
import time


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("ATPInteractionAnalysisDemo")
    
sc = SparkContext(conf = conf)


# ## Read PDB in MMTF format

# In[3]:


path = "../../resources/mmtf_full_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by sequence identity subset

# In[4]:


seqId = 40
resolution = 2.0

pdb = pdb.filter(Pisces(seqId, resolution))


# ## Find ATP interactions within 3 Angstroms
# 
# ![ATPInteraction](./figures/atp-dist2.jpg)

# In[5]:


finder = groupInteractionExtractor("ATP", 3)

interactions = finder.get_dataset(pdb).cache()


# In[6]:


interactions = interactions.filter("atom1 LIKE('O%G')")


# ## Show the data schema of the dataset and some data

# In[7]:


interactions.printSchema()

interactions.show(20)


# ## Count number of interactions

# In[8]:


n = interactions.count()

print(f"Number of interactions: {n}")


# ## Identify top interacting groups

# In[9]:


topGroups = interactions.groupBy("residue2").count()

topGroups.sort("count", ascending = False).show(10) # Sort descending by count


# ## Top interacting groups/atoms types

# In[10]:


topGroupsAndAtoms = interactions.groupBy("residue2","atom2").count()

topGroupsAndAtoms.withColumn("frequency", topGroupsAndAtoms["count"] / n)                 .sort("frequency", ascending = False)                  .show(10)


# # Terminate Spark

# In[11]:


sc.stop()

