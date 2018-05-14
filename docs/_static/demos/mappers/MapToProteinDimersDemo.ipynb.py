
# coding: utf-8

# # Map To Protein Dimers Demo
# 
# Example demonstrating how to extract protein dimers from PDB entries. This example uses a flatMap function to transform a strucure to its dimers.
# 
# ![Protein Dimers](https://upload.wikimedia.org/wikipedia/commons/thumb/c/ce/Galactose-1-phosphate_uridylyltransferase_1GUP.png/220px-Galactose-1-phosphate_uridylyltransferase_1GUP.png)
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.mappers import StructureToProteinDimers, StructureToBioassembly


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("MapToProteinDimersDemo")
sc = SparkContext(conf = conf)


# ## Download Protein *1STP* in MMTF format

# In[3]:


protein = mmtfReader.download_mmtf_files(["1STP"], sc)


# ## Map protein structure to Bioassembly, then map to protein dimers

# In[4]:


cutoffDistance = 8.0
contacts = 20
useAllAtoms = False
exclusive = True

dimers = protein.flatMap(StructureToBioassembly())                 .flatMap(StructureToProteinDimers(cutoffDistance, contacts, useAllAtoms, exclusive))
    


# ## Count number of structures

# In[5]:


print(f"Number of structures : {dimers.count()}")


# ## Terminate Spark

# In[6]:


sc.stop()

