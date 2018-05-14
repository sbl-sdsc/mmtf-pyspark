
# coding: utf-8

# # Advanced Zinc Interaction Analysis Example
# 
# <img src="./figures/zinc_interaction.png" style="width: 300px;"/>
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from mmtfPyspark.datasets import groupInteractionExtractor
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.webfilters import Pisces


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("advancedZincInteractionDemo")

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

n = interactions.count()

print(f"Number of interactions: {n}")


# ## Show the top 10 interacting group/atom types
# 
# #### Exclude Carbon Interactions

# In[7]:


topGroupsAndAtoms = interactions.filter("element2 != 'C'")                                 .groupBy("residue2","atom2")                                 .count()


# #### Add column with frequency of occurence
# #### Filter out occurrences < 1% 
# #### Sort descending

# In[8]:


topGroupsAndAtoms.withColumn("frequency", topGroupsAndAtoms["count"] / n)                  .filter("frequency > 0.01")                  .sort("frequency", ascending = False)                  .show(20)


# ## Print the top interacting elements
# 
# #### Exclude carbon interactions and group by element 2

# In[9]:


topElements = interactions.filter("element2 != 'C'")                           .groupBy("element2")                           .count()


# #### Add column with frequencey of occurence
# #### Filter out occurence < 1%
# #### sort decending

# In[10]:


topElements.withColumn("frequency", topElements["count"] / n)            .filter("frequency > 0.01")            .sort("frequency", ascending = False)            .show(10)


# In[11]:


interactions.groupBy("element2")             .avg("distance")             .sort("avg(distance)")             .show(10)


# ## Aggregate multiple statistics
# 
# ### NOTE: from pyspark.sql.functions import * required

# In[12]:


interactions.groupBy("element2")             .agg(count("distance"), avg("distance"), min("distance"), max("distance"), kurtosis("distance"))             .show(10)


# ## Terminate Spark

# In[13]:


sc.stop()

