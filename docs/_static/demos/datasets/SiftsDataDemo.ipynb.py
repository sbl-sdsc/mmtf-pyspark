
# coding: utf-8

# # SIFTS Data Demo
# 
# This demo shows how to query PDB annotations from the SIFTS project.
# 
# The "Structure Integration with Function, Taxonomy and Sequence" is the authoritative source of up-to-date residue-level annotation of structures in the PDB with data available in Uniprot, IntEnz, CATH, SCOP, GO, InterPro, Pfam and PubMed. [link to SIFTS](https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html)
# 
# Data are probided through [Mine 2 SQL](https://pdbj.org/help/mine2-sql)
# 
# Queries can be designed with the interactive [PDBj Mine 2 query service](https://pdbj.org/help/mine2-sql)
# 
# ## Imports

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from mmtfPyspark.datasets import pdbjMineDataset


# ## Configure Spark

# In[2]:


spark = SparkSession.builder                    .master("local[*]")                    .appName("SIFTSDataDemo")                    .getOrCreate()


# ## Get PDB entry to PubMed Id mappings

# In[4]:


pubmedQuery = "SELECT * FROM sifts.pdb_pubmed LIMIT 10"
pubmed = pdbjMineDataset.get_dataset(pubmedQuery)
print(f"First 10 results for query: {pubmedQuery}")
pubmed.show(10)


# ## Get PDB chain to InterPro mappings

# In[9]:


interproQuery = "SELECT * FROM sifts.pdb_chain_interpro LIMIT 10"
interpro = pdbjMineDataset.get_dataset(interproQuery)
print(f"First 10 results for query: {interproQuery}")
interpro.show(10)


# ## Get PDB chain to UniProt mappings

# In[10]:


uniprotQuery = "SELECT * FROM sifts.pdb_chain_uniprot LIMIT 10"
uniprot = pdbjMineDataset.get_dataset(uniprotQuery)
print(f"First 10 results for query: {uniprotQuery}")
uniprot.show(10)


# ## Get PDB chain to taxonomy mappings

# In[11]:


taxonomyQuery = "SELECT * FROM sifts.pdb_chain_taxonomy LIMIT 10"
taxonomy = pdbjMineDataset.get_dataset(taxonomyQuery)
print(f"First 10 results for query: {taxonomyQuery}")
taxonomy.show(10)


# ## Get PDB chain to PFAM mappings

# In[12]:


pfamQuery = "SELECT * FROM sifts.pdb_chain_pfam LIMIT 10"
pfam = pdbjMineDataset.get_dataset(pfamQuery)
print(f"First 10 results for query: {pfamQuery}")
pfam.show(10)


# ## Get PDB chain to CATH mappings

# In[4]:


pubmedQuery = "SELECT * FROM sifts.pdb_pubmed LIMIT 10"
pubmed = pdbjMineDataset.get_dataset(pubmedQuery)
print(f"First 10 results for query: {pubmedQuery}")
pubmed.show(10)


# ## Get PDB chain to SCOP mappings

# In[13]:


scopQuery = "SELECT * FROM sifts.pdb_chain_scop_uniprot LIMIT 10"
scop = pdbjMineDataset.get_dataset(scopQuery)
print(f"First 10 results for query: {scopQuery}")
scop.show(10)


# ## Get PDB chain to Enzyme classification (EC) mappings

# In[14]:


enzymeQuery = "SELECT * FROM sifts.pdb_chain_enzyme LIMIT 10"
enzyme = pdbjMineDataset.get_dataset(enzymeQuery)
print(f"First 10 results for query: {enzymeQuery}")
enzyme.show(10)


# ## Get PDB chain to Gene Ontology term mappings

# In[15]:


goQuery = "SELECT * FROM sifts.pdb_chain_go LIMIT 10"
go = pdbjMineDataset.get_dataset(goQuery)
print(f"First 10 results for query: {goQuery}")
go.show(10)


# ## Terminate Spark

# In[23]:


spark.stop()

