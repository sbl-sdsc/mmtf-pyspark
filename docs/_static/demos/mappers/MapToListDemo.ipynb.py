
# coding: utf-8

# # Map To List Demo
# 
# This example shows how to filter pdb proteins by X-Ray Diffraction, and store information (protein name, resolution, rFree, rWork) of the results in a list
# 
# ## Imports

# In[8]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.filters import ExperimentalMethods
from mmtfPyspark.io import mmtfReader


# ## Configure Spark

# In[9]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("MapToListDemo")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files and sample a small fraction

# In[10]:


path = "../../resources/mmtf_full_sample/"
fraction = 0.001
seed = 123

pdb = mmtfReader.read_sequence_file(path, sc, fraction = fraction, seed = seed)


# ## Filter by X-Ray Diffraction experimental method

# In[11]:


pdb = pdb.filter(ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION))


# ## Map results to a list of information, and print each list

# In[12]:


pdb.map(lambda t: [t[0], t[1].resolution, t[1].r_free, t[1].r_work]).collect()


# ## Terminate Spark

# In[13]:


sc.stop()

