
# coding: utf-8

# # Map Reduce Example
# 
# This demo shows how to use Map and Reduce to count the total number of atoms in PDB
# 
# ## Import

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("MapReduceExample")
sc = SparkContext(conf = conf)


# # Read in MMTF files

# In[3]:


path = "../../resources/mmtf_full_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# # Count number of atoms
# 
# 1) Map each mmtf structure to it's number of atoms
# 
# 2) Count total nubmer of atoms using reduce

# In[4]:


numAtoms = pdb.map(lambda t: t[1].num_atoms).reduce(lambda a,b: a+b)

print(f"Total number of atoms in PDB: {numAtoms}")


# # Terminate Spark

# In[5]:


sc.stop()

