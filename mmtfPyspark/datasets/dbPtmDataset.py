#!/user/bin/env python
__author__ = "Peter Rose"
__version__ = "0.2.0"

from enum import Enum

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import split


class PtmType(Enum):
    ACETYLATION = 'Acetylation'
    PHOSPHORYLATON = 'Phosphorylation'
    UBIQUITINATION = 'Ubiquitination'
    SUCCINYLATION = 'Succinylation'
    METHYLATION = 'Methylation'
    MALONYLATION = 'Malonylation'
    N_LINKEDGLYCOSYLATION = 'N-linkedGlycosylation'
    O_LINKEDGLYCOSYLATION = 'O-linkedGlycosylation'
    SUMOYLATION = 'Sumoylation'
    S_NITROSYLATION = 'S-nitrosylation'
    GLUTATHIONYLATION = 'Glutathionylation'
    AMIDATION = 'Amidation'
    PYRROLIDONECARBOXYLICACID = 'PyrrolidoneCarboxylicAcid'
    PALMITOYLATION = 'Palmitoylation'
    GAMMA_CARBOXYGLUTAMICACID = 'Gamma-carboxyglutamicAcid'
    CROTONYLATION = 'Crotonylation'
    OXIDATION = 'Oxidation'
    MYRISTOYLATION = 'Myristoylation'
    C_LINKEDGLYCOSYLATION = 'C-linkedGlycosylation'
    SULFATION = 'Sulfation'
    FORMYLATION = 'Formylation'
    CITRULLINATION = 'Citrullination'
    GPI_ANCHOR = 'GPI-anchor'
    NITRATION = 'Nitration'
    S_DIACYLGLYCEROL = 'S-diacylglycerol'
    LIPOYLATION = 'Lipoylation'
    CARBAMIDATION = 'Carbamidation'
    NEDDYLATION = 'Neddylation'
    PYRUVATE = 'Pyruvate'
    S_LINKEDGLYCOSYLATION = 'S-linkedGlycosylation'


# location of dbPTM data
DB_PTM_URL = "http://dbptm.mbc.nctu.edu.tw/download/experiment/"
# location of cached dataset (temporary location for prototyping)
CACHED_FILE_URL = "https://github.com/sbl-sdsc/mmtf-data/raw/master/data/dbptm.orc.lzo"
FILENAME = "dbptm.orc.lzo"


def get_ptm_dataset():
    """
    Returns a cached dataset of all post-translational modifications in dbPTM. The dataset
    contains the following columns:
    +-----------+---------+-------------+---------------+--------------------+--------------------+
    |uniProtName|uniProtId|uniProtSeqNum|        ptmType|           pubMedIds|     sequenceSegment|
    +-----------+---------+-------------+---------------+--------------------+--------------------+
    |14310_ARATH|   P48347|          209|Phosphorylation|[23328941, 23572148]|AFDDAIAELDSLNEESY...|
    |14310_ARATH|   P48347|          233|Phosphorylation|          [23572148]|QLLRDNLTLWTSDLNEE...|
    |14310_ARATH|   P48347|          234|Phosphorylation|          [18463617]|LLRDNLTLWTSDLNEEG...|

    Reference:
    dbPTM: http://dbptm.mbc.nctu.edu.tw/

    Kai-Yao Huang, Min-Gang Su, Hui-Ju Kao, Yun-Chung Hsieh, Jhih-Hua Jhong, Kuang-Hao Cheng, Hsien-Da Huang and
    Tzong-Yi Lee. "dbPTM 2016: 10-year anniversary of a resource for post-translational modification of proteins"
    Nucleic Acids Research, Volume 44, Issue D1, 4 January 2016, Pages D435-D446 - [PubMed]

    :return: dataset of PTMs in dbPTM
    """
    spark = SparkSession.builder.getOrCreate()

    # download cached dataset
    spark.sparkContext.addFile(CACHED_FILE_URL)

    # read dataset
    spark.conf.set("spark.sql.orc.impl", "native")
    return spark.read.orc(SparkFiles.get(FILENAME))


def download_ptm_dataset(ptm_type):
    """
    Downloads the specified type of post-translational modifications in dbPTM. The dataset
    contains the following columns:
    +-----------+---------+-------------+---------------+--------------------+--------------------+
    |uniProtName|uniProtId|uniProtSeqNum|        ptmType|           pubMedIds|     sequenceSegment|
    +-----------+---------+-------------+---------------+--------------------+--------------------+
    |14310_ARATH|   P48347|          209|Phosphorylation|[23328941, 23572148]|AFDDAIAELDSLNEESY...|
    |14310_ARATH|   P48347|          233|Phosphorylation|          [23572148]|QLLRDNLTLWTSDLNEE...|
    |14310_ARATH|   P48347|          234|Phosphorylation|          [18463617]|LLRDNLTLWTSDLNEEG...|

    Reference:
    dbPTM: http://dbptm.mbc.nctu.edu.tw/

    Kai-Yao Huang, Min-Gang Su, Hui-Ju Kao, Yun-Chung Hsieh, Jhih-Hua Jhong, Kuang-Hao Cheng, Hsien-Da Huang and
    Tzong-Yi Lee. "dbPTM 2016: 10-year anniversary of a resource for post-translational modification of proteins"
    Nucleic Acids Research, Volume 44, Issue D1, 4 January 2016, Pages D435-D446 - [PubMed]

    :return: dataset of PTMs in dbPTM
    """

    file_name = ptm_type.value + ".txt.gz"
    url = DB_PTM_URL + file_name
    print("downloading: " + url)

    spark = SparkSession.builder.getOrCreate()

    # download cached dataset
    spark.sparkContext.addFile(url)

    # read dataset
    ds = spark.read.option('header', 'true') \
        .option('sep', '\t') \
        .option('comment', '#') \
        .option('inferSchema', 'true') \
        .csv(SparkFiles.get(file_name))

    ds = ds.toDF("uniProtName", "uniProtId", "uniProtSeqNum", "ptmType", "pubMedIds", "sequenceSegment")
    ds = ds.withColumn("pubMedIds", split(ds.pubMedIds, ';'))

    return ds


def download_all_ptm_dataset():
    """
    Downloads all post-translational modifications in dbPTM. The dataset
    contains the following columns:
    +-----------+---------+-------------+---------------+--------------------+--------------------+
    |uniProtName|uniProtId|uniProtSeqNum|        ptmType|           pubMedIds|     sequenceSegment|
    +-----------+---------+-------------+---------------+--------------------+--------------------+
    |14310_ARATH|   P48347|          209|Phosphorylation|[23328941, 23572148]|AFDDAIAELDSLNEESY...|
    |14310_ARATH|   P48347|          233|Phosphorylation|          [23572148]|QLLRDNLTLWTSDLNEE...|
    |14310_ARATH|   P48347|          234|Phosphorylation|          [18463617]|LLRDNLTLWTSDLNEEG...|

    Reference:
    dbPTM: http://dbptm.mbc.nctu.edu.tw/

    Kai-Yao Huang, Min-Gang Su, Hui-Ju Kao, Yun-Chung Hsieh, Jhih-Hua Jhong, Kuang-Hao Cheng, Hsien-Da Huang and
    Tzong-Yi Lee. "dbPTM 2016: 10-year anniversary of a resource for post-translational modification of proteins"
    Nucleic Acids Research, Volume 44, Issue D1, 4 January 2016, Pages D435-D446 - [PubMed]

    :return: dataset of PTMs in dbPTM
    """
    ds = download_ptm_dataset(PtmType.PHOSPHORYLATON)

    for ptm in PtmType:
        if ptm != PtmType.PHOSPHORYLATON:
            ds = ds.union(download_ptm_dataset(ptm))

    return ds


def save_all_ptm_dataset():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set('spark.sql.orc.impl', 'native')
    ds = download_all_ptm_dataset()

    ds.coalesce(1).write.mode('overwrite').option('compression', 'lzo').orc("/Users/peter/work/dbPTM/dbptm.orc.lzo")
