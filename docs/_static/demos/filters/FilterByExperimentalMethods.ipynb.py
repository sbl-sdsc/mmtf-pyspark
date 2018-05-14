
# coding: utf-8

# # Filter By Experimental Methods Demo
# 
# Example how to filter PDB entries by experimental methods.
# 
# 
# [To learn more about experimental methods](http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/methods-for-determining-structure)
# 
# 
# ## Imports

# In[1]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.io import mmtfReader
from mmtfPyspark.filters import ExperimentalMethods
from mmtfPyspark.structureViewer import view_structure


# ## Configure Spark

# In[2]:


conf = SparkConf().setMaster("local[*]")                       .setAppName("FilterByExperimentalMethods")
sc = SparkContext(conf = conf)


# ## Read in MMTF Files

# In[3]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)


# ## Filter by experimental methods
# 
# #### List of supported experimental methods
# 
# * ExperimentalMethods.ELECTRON_CRYSTALLOGRAPHY
# * ExperimentalMethods.ELECTRON_MICROSCOPY
# * ExperimentalMethods.ERP
# * ExperimentalMethods.FIBER_DIFFRACTION
# * ExperimentalMethods.FLUORESCENCE_TRANSFER
# * ExperimentalMethods.INFRARED_SPECTROSCOPY
# * ExperimentalMethods.NEUTRON_DIFFRACTION
# * ExperimentalMethods.POWDER_DIFFRACTION
# * ExperimentalMethods.SOLID_STATE_NMR
# * ExperimentalMethods.SOLUTION_NMR
# * ExperimentalMethods.SOLUTION_SCATTERING
# * ExperimentalMethods.THEORETICAL_MODEL
# * ExperimentalMethods.X_RAY_DIFFRACTION

# In[4]:


pdb = pdb.filter(ExperimentalMethods(ExperimentalMethods.NEUTRON_DIFFRACTION, ExperimentalMethods.X_RAY_DIFFRACTION))


# ## Print out entries

# In[5]:


filtered_structures = pdb.keys().collect()

print(filtered_structures)


# ## Visualize 3D structures of filtered structures

# In[6]:


view_structure(filtered_structures)


# ## Terminate Spark 

# In[7]:


sc.stop()

