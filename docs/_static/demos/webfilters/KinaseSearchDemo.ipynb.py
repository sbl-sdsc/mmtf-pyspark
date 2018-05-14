
# coding: utf-8

# # Author Search Demo
# 
# ![pdbj](https://pdbj.org/content/default.svg)
# 
# Example query for human protein-serine/threonine kinases using SIFTS data retrieved with PDBj Mine 2 webservices.
# 
# 
# ## References
# 
# The "Structure Integration with Function, Taxonomy and Sequence" is the authoritative source of up-to-date residue-level annotation of structures in the PDB with data available in UniProt, IntEnz, CATH, SCOP, GO, InterPro,Pfam and PubMed.
# [SIFTS](https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html) 
# 
# Data are provided through: 
# [Mine 2 SQL](https://pdbj.org/help/mine2-sql)
# 
# Queries can be designed with the interactive
# [PDBj Mine 2 query service](https://pdbj.org/mine/sql)
# 
# 
# 
# ## Imports

# In[2]:


from pyspark import SparkConf, SparkContext
from mmtfPyspark.webfilters import PdbjMineSearch
from mmtfPyspark.mappers import StructureToPolymerChains
from mmtfPyspark.io import mmtfReader


# ## Configure Spark Context

# In[3]:


conf = SparkConf().setMaster("local[*]")                   .setAppName("KinaseDemo")
sc = SparkContext(conf = conf)


# ## Query for human protein-serine/threonine kinases using SIFTS data

# In[4]:


sql = "SELECT t.pdbid, t.chain FROM sifts.pdb_chain_taxonomy AS t  "            + "JOIN sifts.pdb_chain_enzyme AS e ON (t.pdbid = e.pdbid AND t.chain = e.chain) "            + "WHERE t.scientific_name = 'Homo sapiens' AND e.ec_number = '2.7.11.1'"


# ## Read PDB and filter by author

# In[6]:


path = "../../resources/mmtf_reduced_sample/"

pdb = mmtfReader.read_sequence_file(path, sc)                 .flatMap(StructureToPolymerChains())                 .filter(PdbjMineSearch(sql))

print(f"Number of entries matching query: {pdb.count()}")


# ## Terminate Spark Context

# In[7]:


sc.stop()

