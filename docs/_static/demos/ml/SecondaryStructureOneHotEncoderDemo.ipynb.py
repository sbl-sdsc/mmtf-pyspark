
# coding: utf-8

# # Secondary Structure Property Encoder Demo
# 
# This demo creates a dataset of sequence segments dericed from a non-redundant set. The dataset contains the sequence segment, the DSSP Q8 and DSSP Q3 code of the center residue in a seuqnce segment, and a one-hot encoding of the sequence segment.
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.ml import ProteinSequenceEncoder
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.datasets import secondaryStructureSegmentExtractor
from mmtfPyspark.webfilters import Pisces
from mmtfPyspark.io import mmtfReader


# ## Configure Spark Context

# In[2]:


conf = SparkConf()             .setMaster("local[*]")             .setAppName("SecondaryStructureOneHotEncoderDemo")

sc = SparkContext(conf = conf)


#  ## Read MMTF Hadoop sequence file and 
#  
#  Create a non-redundant set(<=20% seq. identity) of L-protein chains

# In[3]:


path = "../../resources/mmtf_reduced_sample/"
sequenceIdentity = 20
resolution = 2.0
fraction = 0.1
seed = 123

pdb = mmtfReader         .read_sequence_file(path, sc)         .flatMap(StructureToPolymerChains())         .filter(Pisces(sequenceIdentity, resolution))         .filter(ContainsLProteinChain())         .sample(False, fraction, seed)


# ## Get content

# In[4]:


segmentLength = 11
data = secondaryStructureSegmentExtractor.get_dataset(pdb, segmentLength).cache()
print(f"original data   : {data.count()}")


# ## Drop Q3 and sequence duplicates

# In[5]:


data = data.dropDuplicates(["labelQ3", "sequence"]).cache()
print(f"- duplicate Q3/seq  : {data.count()}")


# ## Drop sequence duplicates

# In[6]:


data = data.dropDuplicates(["sequence"])
print(f"- duplicate seq  : {data.count()}")


# ## Property Encoding

# In[7]:


encoder = ProteinSequenceEncoder(data)
data = encoder.one_hot_encode()

data.printSchema()
data.show(5, False)


# ## Terminate Spark Context

# In[8]:


sc.stop()

