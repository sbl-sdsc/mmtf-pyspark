
# coding: utf-8

# # Swiss Prot Sequence To Word2Vec
# 
# This demo generates Word2Vector models from protein sequences in UniProt using overlapping n-grams.
# 
# ![SwissProt](http://swift.cmbi.ru.nl/teach/SWISS/IMAGE/swissprot2.gif)
# 
# [UniProt](http://www.uniprot.org/)
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.ml import ProteinSequenceEncoder
from mmtfPyspark.datasets import uniProt


# ## Configure Spark Context

# In[2]:


conf = SparkConf()             .setMaster("local[*]")             .setAppName("SwissProtSequenceWord2VecEncodeDemo")

sc = SparkContext(conf = conf)


# ## Get Swiss Prot dataset from UniProt

# In[3]:


data = uniProt.get_dataset(uniProt.SWISS_PROT)

# Make sure there are no empty sequence records
data = data.na.drop(subset = ["sequence"])

data.show(10, False)


# ## Generate Word2Vec model

# In[4]:


segmentLength = 11
n = 2
windowSize = (segmentLength - 1)/2
vectorSize = 50

# take 10 rows of data for demo
data = data.limit(10)
# add Word2Vec encoded feature vector
encoder = ProteinSequenceEncoder(data)
# overlapping_ngram_word2vec_encode uses keyword attributes
data = encoder.overlapping_ngram_word2vec_encode(n=n , windowSize=windowSize, vectorSize=vectorSize).cache()


# ## Example of output rows

# In[5]:


data.head(2)


# ## Terminate Spark Context

# In[6]:


sc.stop()

