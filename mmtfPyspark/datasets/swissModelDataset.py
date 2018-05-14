#!/user/bin/env python
'''swissModelDataset

This module provides access to SWISS-MODEL datasets containing homology models.

References
----------
- SWISS-MODEL API: https://swissmodel.expasy.org/docs/repository_help#smr_api
- Bienert S, Waterhouse A, de Beer TA, Tauriello G, Studer G, Bordoli L,
  Schwede T (2017). The SWISS-MODEL Repository - new features and
  functionality, Nucleic Acids Res. 45(D1):D313-D319.  https://dx.doi.org/10.1093/nar/gkw1132
- Biasini M, Bienert S, Waterhouse A, Arnold K, Studer G, Schmidt T, Kiefer F,
  Gallo Cassarino T, Bertoni M, Bordoli L, Schwede T(2014). The SWISS-MODEL
  Repository - modelling protein tertiary and quaternary structure using
  evolutionary information, Nucleic Acids Res. 42(W1):W252–W258.  https://doi.org/10.1093/nar/gku340

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import requests
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

SWISS_MODEL_REST_URL = "https://swissmodel.expasy.org/repository/uniprot/"
SWISS_MODEL_PROVIDER = ".json?provider=swissmodel"
PDB_PROVIDER = ".json?provider=pdb"


def get_swiss_models(uniProtIds):
    '''Downloads metadata for SWISS-MODEL homology models for alist of
    UniProtIds. The original data schema is flatterened into a row-based schema.

    Examples
    --------
    >>> uniProtIds = ["P36575", "P24539", "O00244"]
    >>> ds = swissProtDataset.get_swiss_models(uniProtIds)
    >>> ds.show()
    +------+--------+----+---+-----+----------+----+--------+-----------+--------+--------+--------+----------+-----------+
    |    ac|sequence|from| to|qmean|qmean_norm|gmqe|coverage|oligo-state|  method|template|identity|similarity|coordinates|
    +------+--------+----+---+-----+----------+----+--------+-----------+--------+--------+-- -----+----------+-----------+
    |P36575|MSKVF...|   2|371|-3.06|0.66345522|0.75|0.953608|    monomer|Homology|1suj.1.A|68.66484|0.50463312|https://...|
    |P24539|MLSRV...|  76|249|-2.51|0.67113881|0.65|0.679687|    monomer|Homology|5ara.1.S|84.48275|0.54788881|https://...|
    |O00244|MPKHE...|   1| 68| 1.04|0.84233218|0.98|     1.0| homo-2-mer|Homology|1fe4.1.A|   100.0|0.60686457|https://...|
    +------+--------+----+---+-----+----------+----+--------+-----------+--------+--------+--------+----------+-----------+

    Parameters
    ----------
    uniProtIds : list
       list of UniProt Ids

    Returns
    -------
    dataset
       SwissModel dataset
    '''

    dataset = get_swiss_models_raw_data(uniProtIds)
    return _flatten_dataset(dataset)


def get_swiss_models_raw_data(uniProtIds):
    '''Downloads the raw metadata for SWISS-MODEL homology models. This dataset
    is in the original data schema as downloaded from SWISS-MODEL.

    Parameters
    ----------
    uniProtIds : list
       list of UniProt Ids

    Returns
    -------
    dataset
       SwissModel dataset in original data schema
    '''

    paths = []
    for uniProtId in uniProtIds:
        url = SWISS_MODEL_REST_URL + uniProtId + SWISS_MODEL_PROVIDER
        req = requests.get(url)

        inputStream = req.content
        # TODO temporary solution for inability to retrive data
        if (len(inputStream) - len(uniProtId)) < 103:
            print(f"WARNING: Counld not load data for: {uniProtId}")
            continue

        # save data to temporary file requires as input to dataset reader
        paths.append(_save_temp_file(inputStream.decode("utf-8")))

    # load temporary JSON files into Spark dataset
    dataset = _read_json_files(paths)
    return dataset


def _flatten_dataset(ds):
    '''Flattens the original hierarchical data schema into a simple row-based
    schema. Some less useful data are excluded.

    Parameters
    ----------
    ds : dataset
       the original spark dataset

    Returns
    -------
    dataset
       flattened dataset
    '''

    ds = ds.withColumn("structures", explode(ds.result.structures))
    return ds.select(col("query.ac"), col("result.sequence"), \
                     col("structures.from"), col("structures.to"), \
                     col("structures.qmean"), col("structures.qmean_norm"), \
                     col("structures.gmqe"), col("structures.coverage"), \
                     col("structures.oligo-state"), col("structures.method"), \
                     col("structures.template"), col("structures.identity"), \
                     col("structures.similarity"), col("structures.coordinates"),\
                     col("result.md5"), col("structures.md5"))

def _save_temp_file(inputStream):
    '''Saves tabular report as a temporary CSV file

    Parameters
    ----------
    inputStream : str
       inputStream from swiss model

    Returns
    -------
    str
       path to the tempfile
    '''
    tempFile = tempfile.NamedTemporaryFile(delete=False)
    with open (tempFile.name, "w") as t:
        t.writelines(inputStream)
    return tempFile.name


def _read_json_files(paths):
    '''Reads a list of json files to Spark dataset

    Parameters
    ----------
    paths : list
       a list of paths to temporary json files

    Returns
    -------
    dataset
       a sparkdataset
    '''
    spark = SparkSession.builder.getOrCreate()
    dataset = spark.read \
                   .format("json") \
                   .load(paths)
    return dataset
