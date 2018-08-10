#!/user/bin/env python
__author__ = "Peter Rose"
__version__ = "0.2.0"

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, explode, lit, split, upper

# location of dbPTM data
URL = "https://cdn.rcsb.org/resources/protmod/protmod.tsv.gz"
FILE_NAME = "protmod.tsv.gz"


def get_ptm_dataset():
    """
    Retrieves protein modifications present in the Protein Data Bank. Protein Modification
    data are updated weekly in sync with the PDB's weekly update cycle.

    Example:

    >>> ptms = pdbPtmDataset.get_ptm_dataset()
    >>> # filter by residId for: N4-(N-acetylamino)glucosyl-L-asparagine modifications
    >>> # see: http://pir0.georgetown.edu/cgi-bin/resid?id=AA0151
    >>> ptms = ptms.filter("residId = 'AA0151'")
    >>> ptms.show()

    Returns a cached dataset of all post-translational modifications in the PDB. The dataset
    contains the following columns:
    +----------+---------+-------+---------+-------+----+----------+--------------+
    |pdbChainId|pdbResNum|residue|psimodId |residId|ccId|category  |modificationId|
    +----------+---------+-------+---------+-------+----+----------+--------------+
    |1A0H.B    |373      |ASN    |MOD:00831|AA0151 |NAG |attachment|2             |
    |1A0H.B    |584      |NAG    |MOD:00831|AA0151 |NAG |attachment|2             |
    |1A0H.E    |373      |ASN    |MOD:00831|AA0151 |NAG |attachment|2             |
    |1A0H.E    |584      |NAG    |MOD:00831|AA0151 |NAG |attachment|2             |
    |1A14.N    |476A     |NAG    |MOD:00831|AA0151 |NAG |attachment|2             |

    Reference:
    BioJava-ModFinder: identification of protein modifications in 3D structures
    from the Protein Data Bank (2017) Bioinformatics 33: 2047–2049.
    https://doi.org/10.1093/bioinformatics/btx101">doi:10.1093/bioinformatics/btx101

    :return: dataset of PTMs in PDB
    """
    spark = SparkSession.builder.getOrCreate()

    # download dataset
    spark.sparkContext.addFile(URL)

    # read dataset
    ds = spark.read \
        .option('comment', '#') \
        .option('inferSchema', 'true') \
        .option('delimiter', '\t') \
        .csv(SparkFiles.get(FILE_NAME))

    # add column names
    ds = ds.toDF('pdbId', 'chainId', 'modificationId', 'category', 'ccId', 'psimodId', 'residId', 'unused', 'residues')

    # split residue columns into an array
    ds = ds.withColumn('residues', split(ds.residues, ';'))

    # flatten array into individual rows
    ds = ds.withColumn('residues', explode(ds.residues))
    ds = ds.filter("residues != ''")

    # split comma-separated residue information into separate fields
    ds = ds.withColumn('residues', split(ds.residues, ','))
    ds = ds.withColumn('chainId', ds.residues[0])
    ds = ds.withColumn('pdbResNum', ds.residues[1])
    ds = ds.withColumn('residue', ds.residues[3])
    ds = ds.drop(ds.residues)

    # convert PDB Ids to upper case to be consistent with MMTF-Spark convention.
    ds = ds.withColumn('pdbId', upper(ds.pdbId))

    # add a structureChainId column required for joining with other chain-based datasets
    ds = ds.withColumn('pdbChainId', concat(ds.pdbId, lit('.'), ds.chainId))

    return ds.select(ds.pdbChainId, ds.pdbResNum, ds.residue, ds.psimodId, ds.residId, ds.ccId, ds.category,
                     ds.modificationId)
