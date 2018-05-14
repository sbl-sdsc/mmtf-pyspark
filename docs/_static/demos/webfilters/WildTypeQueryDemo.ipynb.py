
# coding: utf-8

# # Wild Type Query Demo
# 
# This demo selects protein sequences that do not contain mutations in comparison with the reference UniProt sequences.
# 
# Expression tags: Some PDB entries include expression tags that were added during the experiment. Select "No" to filter out sequences with expression tags. Percent coverage of UniProt sequence: PDB entries may contain only a portion of the referenced UniProt sequence. The "Percent coverage of UniProt sequence" option defines how much of a UniProt sequence needs to be contained in a PDB entry.
# 
# 
# ## Imports

# In[5]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.webfilters import WildTypeQuery


# ## Configure Spark

# In[6]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("wildTypeQuery")
sc = SparkContext(conf = conf)


# ## Read in Hadoop Sequence Files and filter by WildType

# In[7]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)                 .filter(WildTypeQuery(includeExpressionTags = True, percentSequenceCoverage = WildTypeQuery.SEQUENCE_COVERAGE_95))


# ## Count results and show top 5 structures

# In[8]:


count = pdb.count()

print(f"Number of structures after filtering : {count}")

pdb.top(5)


# ## Terminate Spark

# In[9]:


sc.stop()

