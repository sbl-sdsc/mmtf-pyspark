#!/user/bin/env python
'''drugBankDatset.py

This module provides access to DrugBank containing drug structure and drug target
imformation. These datasets contain identifiers and names for integration with
other data resources.

References
----------
- Drug Bank. https://www.drugbank.ca
- Wishart DS, et al., DrugBank 5.0: a major update to the DrugBank database for 2018.
  Nucleic Acids Res. 2017 Nov 8. https://dx.doi.org/10.1093/nar/gkx1037

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import requests
import tempfile
import io
from zipfile import ZipFile
from io import BytesIO
from pyspark.sql import SparkSession

DRUG_GROUP = ['ALL', 'APPROVED', 'EXPERIMENTAL', 'NUTRACEUTICAL', 'ILLICIT'\
              'WITHDRAWN', 'INVESTIGATIONAL']
DRUG_TYPE = ['SMALL_MOLECULE', 'BIOTECH']
BASE_URL = "https://www.drugbank.ca/releases/latest/downloads/"
BUFFER = 2048


def get_open_drug_links():
    '''Downloads the DrugBank Open Data dataset with drug structure external
    links and identifiers. See DrugBank.

    This dataset contains drug common names, synonyms, CAS numbers, and
    Standard InChIKeys.

    The DrugBank Open Data dataset is a public domain dataset that can be
    used freely in your application or project (including commercial use).
    It is released under a Creative Common’s CC0 International License.

    References
    ----------
    Open Data dataset. https://www.drugbank.ca/releases/latest#open-data

    Examples
    --------
    Get DrugBank open dataset:

    >>> openDrugLinks = DrugBankDataset.get_open_drug_links()
    >>> openDrugLinks.show()
    +----------+--------------------+-----------+--------------------+
    |DrugBankID|          Commonname|        CAS|    StandardInChIKey|
    +----------+--------------------+-----------+--------------------+
    |   DB00006|         Bivalirudin|128270-60-0|OIRCOABEOLEUMC-GE...|
    |   DB00014|           Goserelin| 65807-02-5|BLCLNMBMMGCOAS-UR...|
    +----------+--------------------+-----------+--------------------+

    Returns
    -------
    dataset
       DrugBank Dataset

    '''

    url = BASE_URL + "all-drugbank-vocabulary"
    return get_dataset(url)


def get_drug_links(drugGroup, username, password):
    '''Downloads drug structure external links and identifiers from DrugBank.
    Either all or subsets of data can be downloaded by specifying the
    DrugGroup:
        ALL, APPROVED, EXPERIMENTAL, NUTRACEUTICAL, ILLLICT, WITHDRAWN,
        INVESTIGATIONAL.

    The structure external links datasets include drug structure information
    in the form of InChI/InChI Key/SMILES as well as identifiers for other
    drug-structure resources (such as ChEBI, ChEMBL,ChemSpider, BindingDB,
    etc.). Included in each dataset is also the PubChem Compound ID (CID) and
    the particular PubChem Substance ID (SID) for the given DrugBank record.

    These DrugBank datasets are released under a Creative Common’s
    Attribution-NonCommercial 4.0 International License. They can be used
    freely in your non-commercial application or project. A DrugBank user
    account and authentication is required to download these datasets.

    References
    ----------
    External Drug Links:
        https://www.drugbank.ca/releases/latest#external-links

    Examples
    --------
    Get dataset of external links and identifiers of approved drugs:

    >>> username = "<your DrugBank username>"
    >>> String password = "<your DrugBank password>"
    >>> drugLinks = get_drug_links("APPROVED", username, password)
    >>> drugLinks.show()

    Parameters
    ----------
    durgGroup : str
       specific dataset to be downloaded, has to be in the pre-defined DURG_GROUP list
    usesrname : str
       DrugBank username
    password : str
       DrugBank password

    Returns
    -------
    dataset
       DrugBank Dataset
    '''

    if drugGroup.upper() not in DRUG_GROUP:
        raise ValueError("drugGroup not in pre-defined durgGroups")

    url = BASE_URL + drugGroup + "-structure-links"
    return get_dataset(url, username, password)


def get_drug_target_links(drug, username, password):
    '''Downloads drug target external links and identifiers from DrugBank.
    Either all or subsets of data can be downloaded by specifying the
    DrugGroup:
        ALL, APPROVED, EXPERIMENTAL, NUTRACEUTICAL, ILLLICT, WITHDRAWN,
        INVESTIGATIONAL.
    OR DrugType:
        SMALL_MOLECULE, BIOTECH.

    The drug target external links datasets include drug name, drug type
    (small molecule, biotech), UniProtID and UniProtName.

    These DrugBank datasets are released under the Creative Common’s
    Attribution-NonCommercial 4.0 International License. They can be used
    freely in your non-commercial application or project. A DrugBank user
    account and authentication is required to download these datasets.

    References
    ----------
    Target Drug-UniProt:
        https://www.drugbank.ca/releases/latest#external-links

    Examples
    --------
    Get dataset of drug target external links and identifiers of all drugs in DrugBank:

    >>> username = "<your DrugBank username>"
    >>> password = "<your DrugBank password>"
    >>> drugTargetLinks = get_drug_target_links("ALL",
    ...                                         username,
    ...                                         password)
    >>> drugTargetLinks.show()

    Parameters
    ----------
    durg : str
       specific dataset to be downloaded, has to be either in
       the DrugGroup list OR DrugType list.
    usesrname : str
       DrugBank username
    password : str
       DrugBank password

    Returns
    -------
    dataset
       DrugBank Dataset
    '''

    if drug.upper() in DRUG_GROUP or drug.upper() in DRUG_TYPE:
        url = BASE_URL + "target-" + drug + "-uniprot-links"
    else:
        raise ValueError("drug not in pre-defined durgGroups or drugTypes")

    return get_dataset(url, username, password)


def get_dataset(url, username=None, password=None):
    '''Downloads a DrugBank dataset

    Parameters
    ----------
    url : str
       DrugBank dataset download links
    username : str, optional
       DrugBank username <None>
    password : str, optional
       DrugBank password <None>

    Returns
    -------
    dataset
       DrugBank dataset
    '''
    if username is None and password is None:
        # get input stream to first zip entry
        req = requests.get(url)
    else:
        # TODO dataset that requires authentication
        req = requests.get(url, auth=(username, password))
        if req.text ==  'Invalid Email or password.':
                raise ValueError('Invalid Email or password.')

    # Decode and unzip file
    unzipped = _decode_as_zip_input_stream(req.content)

    # save data to a temporary file (Dataset csv reader requires a input
    # file!)
    tempFileName = _save_temp_file(unzipped)

    #load temporary CSV file to Spark dataste
    dataset = _read_csv(tempFileName)
    dataset = _remove_spaces_from_column_names(dataset)

    return dataset


def _decode_as_zip_input_stream(content):
    '''Returns an input stream to the first zip file entry

    Parameters
    ----------
    content : inputStream
       inputStream content from request

    Returns
    -------
    inputStream
       unzipped InputStream
    '''

    zipfile = ZipFile(BytesIO(content))
    return [line.decode() for line \
            in zipfile.open(zipfile.namelist()[0]).readlines()]


def _save_temp_file(unzipped):
    '''Saves tabular report as a temporary CSV file

    Parameters
    ----------
    unzipped : list
       list of unzipped content

    Returns
    -------
    str
       path to the tempfile
    '''
    tempFile = tempfile.NamedTemporaryFile(delete=False)
    with io.open(tempFile.name, "w", encoding='utf-8') as t:
       t.writelines(unzipped)

    return tempFile.name


def _read_csv(inputFileName):
    '''Reads CSV file into Spark dataset

    Parameters
    ----------
    fileName : str
       name of the input csv fileName

    Returns
    -------
    dataset
       a spark dataset
    '''
    spark = SparkSession.builder.getOrCreate()
    dataset = spark.read.format("csv") \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(inputFileName)
    return dataset


def _remove_spaces_from_column_names(original):
    '''Remove spaces from column names to ensure compatibility with parquet

    Parameters
    ----------
    original : dataset
       the original dataset

    Returns
    -------
    dataset
       dataset with columns renamed
    '''

    for existingName in original.columns:
        newName = existingName.replace(' ','')
        # TODO: double check dataset "withColumnRenamed" funciton
        original = original.withColumnRenamed(existingName,newName)

    return original
