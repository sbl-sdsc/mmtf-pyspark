
# coding: utf-8

# # Jpred Demo
# 
# This demo shows how to create and query a Jpred dataset.
# 
# ![Jpred](./figures/Jpred.png)
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext, SQLContext
from mmtfPyspark.datasets import jpredDataset


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("JpredDemo")
sc = SparkContext(conf = conf)


# ## Get Jpred Datasets

# In[3]:


res = jpredDataset.get_dataset()


# ## Display Jpred Dataset

# In[4]:


res.show(20, False)


# ## Coalesce

# In[5]:


res = res.coalesce(1)


# ## Save to a local JSON file
# 
# ### This line of code will overwrite exsisting file or directory

# In[6]:


res.write.mode("overwrite").format("json").save("Local directory to save your JSON file")


# ## Terminate Spark

# In[7]:


sc.stop()

