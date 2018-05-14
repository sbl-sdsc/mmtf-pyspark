
# coding: utf-8

# # Write MMTF Subset Demo
# 
# Simple example writting a subset of mmtf files
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader, mmtfWriter
from mmtfPyspark.filters import ExperimentalMethods, Resolution, RFree
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("WriteMMTFCustomSubset")
sc = SparkContext(conf = conf)


# ## Read in a fractions of entries from a local Hadoop Sequence File

# In[3]:


path = "../../resources/mmtf_full_sample/"
fraction= 0.5
seed = 123

pdb = mmtfReader.read_sequence_file(path, sc, fraction = fraction, seed = seed)

count = pdb.count()

print(f'number of pdb entries read : {count}')


# ## Retain high resolution X-ray structures

# In[4]:


pdb = pdb.filter(ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION))          .filter(Resolution(0,2.0))          .filter(RFree(0,2.0))

print(f'number of pdb entries left : {pdb.count()}')


# ## Visualize Structures

# In[5]:


structures = pdb.keys().collect()
view_structure(structures)


# ## Save this subset in a Hadoop Sequence File

# In[7]:


write_path = "./mmtf_subset_xray"

# Reduce RDD to 8 partitiions
pdb = pdb.coalesce(8)
mmtfWriter.write_sequence_file(write_path, sc, pdb)


# ## Terminate Spark

# In[8]:


sc.stop()

