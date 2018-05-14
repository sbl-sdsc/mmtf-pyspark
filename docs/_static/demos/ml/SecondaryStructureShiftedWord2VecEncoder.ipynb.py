
# coding: utf-8

# # Secondary Structure Shifted Word2Vec Encoder
# 
# This demo creates a dataset of sequence segments derived from a non-redundent set. The dataset contains the seuqence segment, the DSSP Q8 and DSSP Q3 code of the center residue in a sequnece segment, and a 3-gram shifted Word2Vec encoding of the seuqnece segment.
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


conf = SparkConf()         .setMaster("local[*]")         .setAppName("secondaryStructureShiftedWord2VecEncodeDemo")
sc = SparkContext(conf = conf)


# ## Read in, filter and sample Hadoop Sequence Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

sequenceIdentity = 20
resolution = 2.0
fraction = 0.1
seed = 123

pdb = mmtfReader         .read_sequence_file(path, sc)         .flatMap(StructureToPolymerChains())         .filter(Pisces(sequenceIdentity, resolution))         .filter(ContainsLProteinChain())         .sample(False, fraction, seed)


# ## Extract Secondary Structure Segments

# In[4]:


segmentLength = 25
data = secondaryStructureSegmentExtractor.get_dataset(pdb, segmentLength).cache()


# ## Add Word2Vec encoded feature vector

# In[6]:


encoder = ProteinSequenceEncoder(data)

windowSize = (segmentLength -1) // 2
vectorSize = 50
# overlapping_ngram_word2vec_encode uses keyword attributes
data = encoder.shifted_3gram_word2vec_encode(windowSize=windowSize, vectorSize=vectorSize).cache()


# ## Show dataset schema and few rows of data

# In[7]:


data.printSchema()
data.show(10, False)


# In[8]:


df = data.toPandas()

df.head(10)


# ## Terminate Spark Context

# In[9]:


sc.stop()

