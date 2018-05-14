
# coding: utf-8

# # Protein Fold Dataset Creator Dmeo
# 
# This Demo is a simple example of using Dataset operations to create a datset
# 
# ## Imports

# In[12]:


from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import col, when
from mmtfPyspark.ml import ProteinSequenceEncoder
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.webfilters import Pisces
from mmtfPyspark.io import mmtfReader


# ## Define addProteinFoldType function

# In[13]:


def add_protein_fold_type(data, minThreshold, maxThreshold):
    '''
    Adds a column "foldType" with three major secondary structure class:
    "alpha", "beta", "alpha+beta", and "other" based upon the fraction of alpha/beta content.

    The simplified syntax used in this method relies on two imports:
        from pyspark.sql.functions import when
        from pyspark.sql.functions import col

    Attributes:
        data (Dataset<Row>): input dataset with alpha, beta composition
        minThreshold (float): below this threshold, the secondary structure is ignored
        maxThreshold (float): above this threshold, the secondary structure is ignored
    '''

    return data.withColumn("foldType",                            when((col("alpha") > maxThreshold) & (col("beta") < minThreshold), "alpha").                            when((col("beta") > maxThreshold) & (col("alpha") < minThreshold), "beta").                            when((col("alpha") > maxThreshold) & (col("beta") > minThreshold), "alpha+beta").                            otherwise("other")                           )


# ## Configure Spark Context

# In[14]:


conf = SparkConf()             .setMaster("local[*]")             .setAppName("ProteinFoldDatasetCreatorDemo")

sc = SparkContext(conf = conf)


# ## Read MMTF Hadoop sequence file
# 
# Create non-redundant set (<=40% seq. identity) if L-protein chains

# In[15]:


path = "../../resources/mmtf_reduced_sample/"
sequenceIdentity = 40
resolution = 2.0

pdb = mmtfReader         .read_sequence_file(path, sc)         .filter(Pisces(sequenceIdentity, resolution))         .flatMap(StructureToPolymerChains())         .filter(Pisces(sequenceIdentity, resolution))         .filter(ContainsLProteinChain())


# ## Get secondary structure content

# In[16]:


data = secondaryStructureExtractor.get_dataset(pdb)


# ## Classify chains by secondary structure type

# In[17]:


minThreshold = 0.05
maxThreshold = 0.15
data = add_protein_fold_type(data, minThreshold, maxThreshold)


# ## Add Word2Vec encoded feature vector

# In[19]:


encoder = ProteinSequenceEncoder(data)
n = 2 # Create 2-grams
windowSize = 25 # 25-amino residue window size for Word2Vec
vectorSize = 50 # dimension of feature vector
# overlapping_ngram_word2vec_encode uses keyword attributes
data = encoder.overlapping_ngram_word2vec_encode(n = n, windowSize = windowSize, vectorSize=vectorSize).cache()

data.printSchema()
data.show(10)


# ## Keep only a subset of relevant fields for futher processing

# In[20]:


data = data.select("structureChainId", "alpha", "beta", "coil", "foldType", "features")

data.show(10)


# ## Terminate Spark Context

# In[21]:


sc.stop()

