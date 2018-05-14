
# coding: utf-8

# # Simple Zinc Interaction Analysis Example
# 
# <img src="./figures/zinc_interaction.png" style="width: 300px;"/>
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.datasets import groupInteractionExtractor
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.webfilters import Pisces


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("simpleZincInteractionDemo")

sc = SparkContext(conf = conf)


# ## Read PDB in MMTF format

# In[3]:


path = "../../resources/mmtf_full_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# # Use only representative structures

# In[4]:


seqId = 40
resolution = 2.0

pdb = pdb.filter(Pisces(seqId, resolution))


# ## Extract proteins with Zn interactions

# In[5]:


finder = groupInteractionExtractor("ZN",3)

interactions = finder.get_dataset(pdb).cache()


# ## List the top 10 residue types that interact with Zn

# In[6]:


interactions.printSchema()

interactions.show(20)

print(f"Number of interactions: {interactions.count()}")


# ## Show the top 10 interacting groups

# In[7]:


interactions.groupBy("residue2")             .count()             .sort("count", ascending = False)             .show(10)


# ## Terminate Spark

# In[8]:


sc.stop()

