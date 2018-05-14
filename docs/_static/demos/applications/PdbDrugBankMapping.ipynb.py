
# coding: utf-8

# # PDB Drug Bank Mapping
# 
# Join PDB, Drug Bank and PDBjMine dataset together 

# ## Imports and variables

# In[1]:


from pyspark import SparkConf, SparkContext                    
from mmtfPyspark.datasets import customReportService, drugBankDataset, pdbjMineDataset
from mmtfPyspark import structureViewer
                                                               
# Create variables                                             
APP_NAME = "MMTF_Spark"                                        
path = "../../resources/mmtf_full_sample/"

# Configure Spark                                              
conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")  
sc = SparkContext(conf=conf)                                   


# ## Download open DrugBank dataset

# In[2]:


drugBank = drugBankDataset.get_open_drug_links()
drugBank.toPandas().head(10)


# ## Filter out DrugBank entries without StandardInChIKey

# In[3]:


drugBank = drugBank.filter(drugBank.StandardInChIKey.isNotNull())
drugBank.toPandas().head(5)


# ## Get PDB ligand annotations

# In[4]:


ligands = customReportService.get_dataset(["ligandId","ligandMolecularWeight","ligandFormula","ligandSmiles","InChIKey"])
ligands.toPandas().head(10)


# ## Filter out DrugBank entries without InChIKey

# In[5]:


ligands = ligands.filter(ligands.InChIKey.isNotNull())
ligands.toPandas().head(5)


# ## Join ligand dataset with DrugBank info by InChIKey

# In[6]:


ligands = ligands.join(drugBank, ligands.InChIKey == drugBank.StandardInChIKey)
ligands.toPandas().head(10)


# ## Show one example per drug molecule

# In[7]:


ligands = ligands.dropDuplicates(["Commonname"])
ligands = ligands.select("structureChainId", "ligandId", "DrugBankID", "Commonname", "ligandMolecularWeight","ligandFormula", "InChIKey", "ligandSmiles")
ligands.sort("Commonname").toPandas().head(10)


# ## Query structures with 2.7.11.1 EC number using PDBjMine

# In[8]:


enzymeQuery = "SELECT * FROM sifts.pdb_chain_enzyme WHERE ec_number = '2.7.11.1'"
enzyme = pdbjMineDataset.get_dataset(enzymeQuery)

print(f"First 10 results for query: {enzymeQuery}")
enzyme.show(10)


# ## Join ligand dataset with PDBjMine dataset with structureChainId

# In[9]:


ligands = ligands.join(enzyme, ligands.structureChainId == enzyme.structureChainId)
print(f"Total number of structures: {ligands.count()}")

df = ligands.toPandas()
df.head()


# ## Visualize protein kinase interaction

# In[11]:


structureViewer.view_binding_site(df.pdbid, df.ligandId, df.chain, 4.0)


# ## Terminate Spark

# In[12]:


sc.stop()

