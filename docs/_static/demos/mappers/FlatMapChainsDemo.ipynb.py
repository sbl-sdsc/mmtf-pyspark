
# coding: utf-8

# # Flat Map Chains Demo
# 
# Example demonstrateing how to extract protein chains from PDB entries. This example uses a flatMap function to transform a structure to its polymer chains.
# 
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.filters import PolymerComposition
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.mappers import StructureToPolymerChains


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("FlatMapChainsDemo")
sc = SparkContext(conf = conf)


# ## Read in MMTF files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## flat map structure to polymer chains, filter by polymer composition and count
# 
# ### Supported polymer composition type:
# 
# ** polymerComposition.AMINO_ACIDS_20  **= ["ALA","ARG","ASN","ASP","CYS","GLN","GLU","GLY","HIS","ILE","LEU","LYS","MET","PHE","PRO","SER","THR","TRP","TYR","VAL"]
# 
# ** polymerComposition.AMINO_ACIDS_22 **= ["ALA","ARG","ASN","ASP","CYS","GLN","GLU","GLY","HIS","ILE","LEU","LYS","MET","PHE","PRO","SER","THR","TRP","TYR","VAL","SEC","PYL"]
# 
# ** polymerComposition.DNA_STD_NUCLEOTIDES **= ["DA","DC","DG","DT"]
# 
# ** polymerComposition.RNA_STD_NUCLEOTIDES **= ["A","C","G","U"]
# 

# In[4]:


count = pdb.flatMap(StructureToPolymerChains(False, True))            .filter(PolymerComposition(PolymerComposition.AMINO_ACIDS_20))            .count()
        
print(f"Chains with standard amino acids: {count}")


# ## Terminate Spark

# In[5]:


sc.stop()

