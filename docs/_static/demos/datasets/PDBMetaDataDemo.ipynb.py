
# coding: utf-8

# # PDB Meta Data Demo
# 
# This demo shows how to query metadata from the PDB archive.
# 
# This exmaple queries the \_citation category. Each category represents a table, and fields represent database columns. [Avalible tables and columns](https://pdbj.org/mine-rdb-docs)
# 
# Example data from 100D.cif
#  * _citation.id                        primary 
#  * _citation.title                     Crystal structure of ...
#  * _citation.journal_abbrev            'Nucleic Acids Res.' 
#  * _citation.journal_volume            22 
#  * _citation.page_first                5466 
#  * _citation.page_last                 5476 
#  * _citation.year                      1994 
#  * _citation.journal_id_ASTM           NARHAD 
#  * _citation.country                   UK 
#  * _citation.journal_id_ISSN           0305-1048 
#  * _citation.journal_id_CSD            0389 
#  * _citation.book_publisher            ? 
#  * _citation.pdbx_database_id_PubMed   7816639 
#  * _citation.pdbx_database_id_DOI      10.1093/nar/22.24.5466 
# 
# Data are probided through [Mine 2 SQL](https://pdbj.org/help/mine2-sql)
# 
# Queries can be designed with the interactive [PDBj Mine 2 query service](https://pdbj.org/help/mine2-sql)
# 
# ## Imports

# In[16]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from mmtfPyspark.datasets import pdbjMineDataset
import matplotlib.pyplot as plt


# ## Configure Spark

# In[2]:


spark = SparkSession.builder                    .master("local[*]")                    .appName("PDBMetaDataDemo")                    .getOrCreate()


# ## Query PDBj Mine
# 
# Query the following fields from the \citation category using PDBj's Mine 2 web service:
#  * journal_abbrev
#  * pdbx_database_id_PubMed
#  * year
# 
# Note: mixed case column names must be quoted and escaped with \
# 

# In[3]:


sqlQuery = "SELECT pdbid, journal_abbrev, \"pdbx_database_id_PubMed\", year from citation WHERE id = 'primary'"

ds = pdbjMineDataset.get_dataset(sqlQuery)


# ## Show first 10 results from query

# In[4]:


ds.show(10, False)


# ## Filter out unpublished entries
# 
# Published entires contain the word "published" in various upper/lower case combinations

# In[5]:


ds = ds.filter("UPPER(journal_abbrev) NOT LIKE '%PUBLISHED%'")


# ## Show the top 10 journals that publish PDB structures

# In[9]:


ds.groupBy("journal_abbrev").count().sort(col("count").desc()).show(10,False)


# ## Filter out entries without a PubMed Id 
# 
# -1 if PubMed Id is not available

# In[10]:


ds = ds.filter("pdbx_database_id_PubMed > 0")

print(f"Entires with PubMed Ids: {ds.count()}")


# ## Show growth of papers in PubMed

# In[14]:


print("PubMed Ids per year: ")
idsPerYear = ds.groupBy("year").count().sort(col("year").desc())
idsPerYear.show(10, False)


# ## Make scatter plot for growth of papers in PubMed

# In[21]:


# Get year and publications as list
year = idsPerYear.select("year").collect()
publications = idsPerYear.select("count").collect()

# Make scatter plot with matplotlib
plt.scatter(year, publications)
plt.xlabel("year")
plt.ylabel("papers")
plt.title("Growth of papers in PubMed each year")


# ## Terminate Spark

# In[23]:


spark.stop()

