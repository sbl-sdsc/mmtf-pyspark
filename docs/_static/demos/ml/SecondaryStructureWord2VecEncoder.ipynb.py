
# coding: utf-8

# # Secondary Structure Word2Vec Encoder
# 
# This demo creates a dataset of sequence segments derived from a non-redundent set. The dataset contains the seuqence segment, the DSSP Q8 and DSSP Q3 code of the center residue in a sequnece segment, and a Word2Vec encoding of the seuqnece segment.
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
import time


# ## Configure Spark Context

# In[2]:


conf = SparkConf()         .setMaster("local[*]")         .setAppName("secondaryStructureWord2VecEncodeDemo")
sc = SparkContext(conf = conf)


# ## Read in, filter and sample Hadoop Sequence Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

sequenceIdentity = 20
resolution = 2.0
fraction = 0.95
seed = 123

pdb = mmtfReader         .read_sequence_file(path, sc)         .flatMap(StructureToPolymerChains())         .filter(Pisces(sequenceIdentity, resolution))         .filter(ContainsLProteinChain())         .sample(False, fraction, seed)


# ## Extract Secondary Structure Segments

# In[4]:


segmentLength = 11
data = secondaryStructureSegmentExtractor.get_dataset(pdb, segmentLength).cache()


# ## Add Word2Vec encoded feature vector

# In[6]:


encoder = ProteinSequenceEncoder(data)

n = 2
windowSize = (segmentLength -1) // 2
vectorSize = 50
# overlapping_ngram_word2vec_encode uses keyword attributes
data = encoder.overlapping_ngram_word2vec_encode(n=n , windowSize=windowSize, vectorSize=vectorSize)


# ## Show dataset schema and few rows of data

# In[7]:


data.printSchema()
data.show(10, False)


# ## Terminate Spark Context

# In[8]:


sc.stop()

