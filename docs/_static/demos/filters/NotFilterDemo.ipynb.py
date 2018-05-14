
# coding: utf-8

# # Not Filter Demo
# 
# Example how to wrap a filter in a not filter to negate a filter
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ContainsDnaChain, ContainsLProteinChain, NotFilter
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[6]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("notFilterExample")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files

# In[7]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by contains L Protein Chain

# In[8]:


structures = pdb.filter(ContainsLProteinChain())


# ## Using Not filter to reverse a filter
# 
# Get entires that does not contain DNA Chains

# In[9]:


structures = structures.filter(NotFilter(ContainsDnaChain()))


# ## Count number of entires

# In[10]:


count = structures.count()

print(f"PDB entires without DNA chains : {count}")


# ## Visualize Structures

# In[11]:


view_structure(structures.keys().collect())


# ## Terminate Spark 

# In[7]:


sc.stop()

