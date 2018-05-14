
# coding: utf-8

# # Sequence Similarity Search Demo
# 
# This demo filters PDB chains by sequence similarity using RCSB PDB webservices.
# 
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.webfilters import SequenceSimilarity
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.io import mmtfReader


# ## Configure Spark Context

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("SequenceSimilaritySearchDemo")
sc = SparkContext(conf = conf)


# ## Read PDB in MMTF format, split into polymer chain, search by sequence similarity, and print sequence found

# In[6]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)                 .flatMap(StructureToPolymerChains())                 .filter(SequenceSimilarity(sequence="NLVQFGVMIEKMTGKSALQYNDYGCYCGIGGSHWPVDQ",                                           searchTool=SequenceSimilarity.BLAST,                                            eValueCutoff=0.001,                                            sequenceIdentityCutoff=40,                                            maskLowComplexity=True))                 .collect()

for pdbId, structure in pdb:
        print(f"{pdbId} :     {structure.entity_list[0]['sequence']}")


# ## Terminate Spark Context

# In[7]:


sc.stop()

