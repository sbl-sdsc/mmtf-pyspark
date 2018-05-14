
# coding: utf-8

# # Secondary Structure Elements Word2Vec Encoder Demo
# 
# This demo creates a dataset by extracting secondary structure elements "H", then encode an overlapping Ngram feature vector
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.ml import ProteinSequenceEncoder
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.datasets import secondaryStructureElementExtractor
from mmtfPyspark.webfilters import Pisces
from mmtfPyspark.io import mmtfReader


# ## Configure Spark Context

# In[2]:


conf = SparkConf()             .setMaster("local[*]")             .setAppName("SecondaryStructureElementsWord2VecEncoderDemo")

sc = SparkContext(conf = conf)


#  ## Read MMTF Hadoop sequence file and 
#  
#  Create a non-redundant set(<=20% seq. identity) of L-protein chains

# In[3]:


path = "../../resources/mmtf_reduced_sample/"
fraction = 0.05
seed = 123

pdb = mmtfReader         .read_sequence_file(path, sc)         .flatMap(StructureToPolymerChains(False, True))         .filter(ContainsLProteinChain())         .sample(False, fraction, seed)


# ## Extract Element "H" from Secondary Structure

# In[4]:


label = "H"
data = secondaryStructureElementExtractor.get_dataset(pdb, label).cache()
print(f"original data   : {data.count()}")
data.show(10, False)


# ## Word2Vec encoded feature Vector

# In[6]:


segmentLength = 11
n = 2
windowSize = (segmentLength-1)/2
vectorSize = 50

encoder = ProteinSequenceEncoder(data)
# overlapping_ngram_word2vec_encode uses keyword attributes
data = encoder.overlapping_ngram_word2vec_encode(n=n, windowSize=windowSize, vectorSize=vectorSize)

data.show(5)


# ## Terminate Spark Context

# In[7]:


sc.stop()

