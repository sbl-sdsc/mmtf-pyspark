
# coding: utf-8

# # Create Representative Set Demo
# 
# Creates an MMTF-Hadoop Sequence file for a Picses representative set of protein chains.
# 
# 
# ## References
# 
# Please cite the following in any work that uses lists provided by PISCES G. Wang and R. L. Dunbrack, Jr. PISCES: a protein sequence culling server. Bioinformatics, 19:1589-1591, 2003.
# [PISCES](http://dunbrack.fccc.edu/PISCES.php)
# 
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader, mmtfWriter
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.filters import PolymerComposition
from mmtfPyspark.webfilters import Pisces


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("CreateRepresentativeSetDemo")
sc = SparkContext(conf = conf)


# ## Read in Haddop Sequence Files

# In[6]:


path = "../../resources/mmtf_full_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by representative protein chains at 40% sequence identity

# In[7]:


sequenceIdentity = 40
resolution = 2.0

pdb = pdb.filter(Pisces(sequenceIdentity, resolution))          .flatMap(StructureToPolymerChains())          .filter(Pisces(sequenceIdentity, resolution))          .filter(PolymerComposition(PolymerComposition.AMINO_ACIDS_20))


# ## Show top 10 structures

# In[8]:


pdb.top(10)


# ## Save representative set

# In[9]:


write_path = f'./pdb_representatives_{sequenceIdentity}'

mmtfWriter.write_sequence_file(write_path, sc, pdb)


# ## Terminate Spark

# In[3]:


sc.stop()

