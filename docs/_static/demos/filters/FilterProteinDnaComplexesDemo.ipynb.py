
# coding: utf-8

# # Filter Protein Dna Complexes Demo
# 
# This example shows how to filter PDB by protein DNA complexes
# 
# ![](./figures/ProteinDnaComplex.png)
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ContainsDnaChain, ContainsLProteinChain
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterProteinDnaComplexDate")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter proteins that cotinas Dna chain and L protein chain
# 
# 1) Retain pdb entires that exclusively contain L-peptide chains
# 2) Retain pdb entries that exclusively contain L-Dna

# In[4]:


structures = pdb.filter(ContainsLProteinChain())          .filter(ContainsDnaChain())


# ## Count number of entires

# In[5]:


count = structures.count()

print(f"Number of entires that contain L-protein and L-DNA: {count}")


# ## Visualize Structures

# In[7]:


structure_names = structures.keys().collect()
view_structure(structure_names)


# ## Terminate Spark 

# In[6]:


sc.stop()

