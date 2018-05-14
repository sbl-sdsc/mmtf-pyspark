
# coding: utf-8

# # Write MMTF Subset Demo
# 
# Simple example writting a subset of mmtf files
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader, mmtfWriter
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("WriteMmtfFullUncompressed")
sc = SparkContext(conf = conf)


# ## Read in a fractions of entries from a local Hadoop Sequence File

# In[3]:


path = "../../resources/mmtf_full_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)

count = pdb.count()

print(f'number of pdb entries read : {count}')


# ## Visualize Structures

# In[5]:


structures = pdb.keys().collect()
view_structure(structures, style='stick')


# ## Save this subset in a Hadoop Sequence File

# In[5]:


write_path = "./mmtf_full_uncompressed"

mmtfWriter.write_sequence_file(write_path, sc, structure=pdb, compressed=False)


# ## Terminate Spark

# In[6]:


sc.stop()

