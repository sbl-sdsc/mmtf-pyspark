
# coding: utf-8

# # Machine learning - Protein Chain Classification
# 
# In this demo we try to classify a protein chain as either an all alpha or all beta protein based on protein sequence. We use n-grams and a Word2Vec representation of the protein sequence as a feature vector.
# 
# [Word2Vec model](https://spark.apache.org/docs/latest/mllib-feature-extraction.html#word2vec)
# 
# [Word2Vec example](https://spark.apache.org/docs/latest/ml-features.html#word2vec)
# 
# ## Imports

# In[17]:


from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.webfilters import Pisces
from mmtfPyspark.filters import ContainsLProteinChain
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.datasets import secondaryStructureExtractor
from mmtfPyspark.ml import ProteinSequenceEncoder, SparkMultiClassClassifier, datasetBalancer   
from pyspark.sql.functions import *
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression, MultilayerPerceptronClassifier, RandomForestClassifier


# ## Configure Spark Context

# In[18]:


conf = SparkConf()             .setMaster("local[*]")             .setAppName("MachineLearningDemo")

sc = SparkContext(conf = conf)


# ## Read MMTF File and create a non-redundant set (<=40% seq. identity) of L-protein clains

# In[19]:


pdb = mmtfReader.read_sequence_file('../../resources/mmtf_reduced_sample/', sc)                 .flatMap(StructureToPolymerChains())                 .filter(Pisces(sequenceIdentity=40,resolution=3.0))


# ## Get secondary structure content

# In[20]:


data = secondaryStructureExtractor.get_dataset(pdb)


# ## Define addProteinFoldType function

# In[21]:


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

    return data.withColumn("foldType",                            when((col("alpha") > maxThreshold) & (col("beta") < minThreshold), "alpha").                            when((col("beta") > maxThreshold) & (col("alpha") < minThreshold), "beta").                            when((col("alpha") > maxThreshold) & (col("beta") > maxThreshold), "alpha+beta").                            otherwise("other")                           )


# ## Classify chains by secondary structure type

# In[22]:


data = add_protein_fold_type(data, minThreshold=0.05, maxThreshold=0.15)


# ## Create a Word2Vec representation of the protein sequences
# 
# **n = 2**     # create 2-grams 
# 
# **windowSize = 25**    # 25-amino residue window size for Word2Vector
# 
# **vectorSize = 50**    # dimension of feature vector

# In[23]:


encoder = ProteinSequenceEncoder(data)
data = encoder.overlapping_ngram_word2vec_encode(n=2, windowSize=25, vectorSize=50).cache()

data.toPandas().head(5)


# ## Keep only a subset of relevant fields for further processing

# In[24]:


data = data.select(['structureChainId','alpha','beta','coil','foldType','features'])


# ## Select only alpha and beta foldType to parquet file

# In[25]:


data = data.where((data.foldType == 'alpha') | (data.foldType == 'beta')) #| (data.foldType == 'other'))

print(f"Total number of data: {data.count()}")
data.toPandas().head()


# ## Basic dataset information and setting

# In[26]:


label = 'foldType'
testFraction = 0.1
seed = 123

vector = data.first()["features"]
featureCount = len(vector)
print(f"Feature count    : {featureCount}")
    
classCount = int(data.select(label).distinct().count())
print(f"Class count    : {classCount}")

print(f"Dataset size (unbalanced)    : {data.count()}")
    
data.groupby(label).count().show(classCount)
data = datasetBalancer.downsample(data, label, 1)
print(f"Dataset size (balanced)  : {data.count()}")
    
data.groupby(label).count().show(classCount)


# ## Decision Tree Classifier

# In[27]:


dtc = DecisionTreeClassifier()
mcc = SparkMultiClassClassifier(dtc, label, testFraction, seed)
matrics = mcc.fit(data)
for k,v in matrics.items(): print(f"{k}\t{v}")


# ## Random Forest Classifier

# In[28]:


rfc = RandomForestClassifier()
mcc = SparkMultiClassClassifier(rfc, label, testFraction, seed)
matrics = mcc.fit(data)
for k,v in matrics.items(): print(f"{k}\t{v}")


# ## Logistic Regression Classifier

# In[29]:


lr = LogisticRegression()
mcc = SparkMultiClassClassifier(lr, label, testFraction, seed)
matrics = mcc.fit(data)
for k,v in matrics.items(): print(f"{k}\t{v}")


# ## Simple Multilayer Perception Classifier

# In[30]:


layers = [featureCount, 64, 64, classCount]
mpc = MultilayerPerceptronClassifier().setLayers(layers)                                           .setBlockSize(128)                                           .setSeed(1234)                                           .setMaxIter(100)
mcc = SparkMultiClassClassifier(mpc, label, testFraction, seed)
matrics = mcc.fit(data)
for k,v in matrics.items(): print(f"{k}\t{v}")


# ## Terminate Spark

# In[31]:


sc.stop()

