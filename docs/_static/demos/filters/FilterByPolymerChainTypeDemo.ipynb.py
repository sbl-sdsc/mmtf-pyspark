
# coding: utf-8

# # Filter By Polymer Chain Type Demo
# 
# Simple exmaple of reading an MMTF Hadoop Sequence file, filtering the entries by polymer chain type, L Protein Chain and D Saccharide Chain, and count the number of entires. This example also show show methods can be chained for a more concise syntax
# 
# #### Supported polymer chain type includes  (Both string and class variable can be used a input parameter)
# 
# * containsPolymerChainType.D_PEPTIDE_COOH_CARBOXY_TERMINUS = "D-PEPTIDE COOH CARBOXY TERMINUS"
# * containsPolymerChainType.D_PEPTIDE_NH3_AMINO_TERMINUS = "D-PEPTIDE NH3 AMINO TERMINUS"
# * containsPolymerChainType.D_PEPTIDE_LINKING = "D-PEPTIDE LINKING"
# * containsPolymerChainType.D_SACCHARIDE = "D-SACCHARIDE"
# * containsPolymerChainType.D_SACCHARIDE_14_and_14_LINKING = "D-SACCHARIDE 1,4 AND 1,4 LINKING"
# * containsPolymerChainType.D_SACCHARIDE_14_and_16_LINKING = "D-SACCHARIDE 1,4 AND 1,6 LINKING"
# * containsPolymerChainType.DNA_OH_3_PRIME_TERMINUS = "DNA OH 3 PRIME TERMINUS"
# * containsPolymerChainType.DNA_OH_5_PRIME_TERMINUS = "DNA OH 5 PRIME TERMINUS"
# * containsPolymerChainType.DNA_LINKING = "DNA LINKING"
# * containsPolymerChainType.L_PEPTIDE_COOH_CARBOXY_TERMINUS = "L-PEPTIDE COOH CARBOXY TERMINUS"
# * containsPolymerChainType.L_PEPTIDE_NH3_AMINO_TERMINUS = "L-PEPTIDE NH3 AMINO TERMINUS"
# * containsPolymerChainType.L_PEPTIDE_LINKING = "L-PEPTIDE LINKING"
# * containsPolymerChainType.L_SACCHARIDE = "L-SACCHARIDE"
# * containsPolymerChainType.L_SACCHARIDE_14_AND_14_LINKING = "L-SACCHARDIE 1,4 AND 1,4 LINKING"
# * containsPolymerChainType.L_SACCHARIDE_14_AND_16_LINKING = "L-SACCHARIDE 1,4 AND 1,6 LINKING"
# * containsPolymerChainType.PEPTIDE_LINKING = "PEPTIDE LINKING"
# * containsPolymerChainType.RNA_OH_3_PRIME_TERMINUS = "RNA OH 3 PRIME TERMINUS"
# * containsPolymerChainType.RNA_OH_5_PRIME_TERMINUS = "RNA OH 5 PRIME TERMINUS"
# * containsPolymerChainType.RNA_LINKING = "RNA LINKING"
# * containsPolymerChainType.NON_POLYMER = "NON-POLYMER"
# * containsPolymerChainType.OTHER = "OTHER"
# * containsPolymerChainType.SACCHARIDE = "SACCHARIDE"
# 
# 
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import *
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterByPolymerChainType")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files, filter and count
# 
# #### * Not filter returns the opposite of a particular filter*

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

structures = mmtfReader.read_sequence_file(path, sc)                   .filter(ContainsPolymerChainType("DNA LINKING", ContainsPolymerChainType.RNA_LINKING))                   .filter(NotFilter(ContainsLProteinChain()))                   .filter(NotFilter(ContainsDSaccharideChain()))

print(f"Number of pure DNA and RNA entires: {structures.count()}")


# ## View Structures

# In[4]:


structure_names = structures.keys().collect()
view_structure(structure_names, style='sphere')


# ## Terminate Spark 

# In[5]:


sc.stop()

