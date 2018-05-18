#!/usr/bin/env python
'''mmtfReader.py: Methods for reading and downloading structures in MMTF file
formats. The data are returned as a PythonRDD with the structure id (e.g. PDB ID)
as the key and the structural data as the value.

Supported operations and file formats:
- Read directory of MMTF-Hadoop sequence files in full and reduced representation
- Download MMTF full and reduced representations using web service (mmtf.rcsb.org)
- Read directory of MMTF files (.mmtf, mmtf.gz)

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import os
import msgpack
import gzip
from mmtfPyspark.utils import MmtfStructure
from mmtf.api import default_api
from os import path, walk
from pyspark.sql import SparkSession
import urllib

text = "org.apache.hadoop.io.Text"
byteWritable = "org.apache.hadoop.io.BytesWritable"


def read_full_sequence_file(pdbId=None, fraction=None, seed=123):
    '''Reads a MMTF-Hadoop Sequence file using the default file location.
    The default file location is determined by :func:`get_mmtf_full_path() <mmtfPyspark.io.mmtfReader.get_mmtf_full_path>`

    To download mmtf files: https://mmtf.rcsb.org/download.html

    Parameters
    ----------
    pdbID : list, optional
       List of structures to read
    fraction : float, optional
       fraction of structure to read
    seed : int, optional
       random seed
    '''
    return read_sequence_file(get_mmtf_full_path(), pdbId, fraction, seed)


def read_reduced_sequence_file(pdbId=None, fraction=None, seed=123):
    '''Reads a MMTF-Hadoop Sequence file using the default file location.
    The default file location is determined by :func:`get_mmtf_reduced_path()
    <mmtfPyspark.io.mmtfReader.get_mmtf_reducedget_mmtf_reduced_path>`

    To download mmtf files: {https://mmtf.rcsb.org/download.htm}

    Parameters
    ----------
    pdbID : list, optional
       List of structures to read
    fraction : float, optional
       fraction of structure to read
    seed : int, optional
       random seed
    '''
    return read_sequence_file(get_mmtf_reduced_path(), pdbId, fraction, seed)


def read_sequence_file(path, pdbId=None, fraction=None, seed=123):
    '''Reads an MMTF Hadoop Sequence File. Can read all files from path,
    randomly rample a fraction, or a subset based on input list.
    See <a href="http://mmtf.rcsb.org/download.html"> for file download information</a>

    Parameters
    ----------
    path : str
       path to file directory
    pdbID : list
       List of structures to read
    fraction : float
       fraction of structure to read
    seed : int
       random seed

    Raises
    ------
    Exception
       file path does not exist

    '''

    if not os.path.exists(path):
        raise Exception("file path does not exist")

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    infiles = sc.sequenceFile(path, text, byteWritable)

    # Read in all structures from a directory
    if (pdbId == None and fraction == None):
        return infiles.map(_call_sequence_file)

    # Read in a specified list of pdbIds
    elif(pdbId != None and fraction == None):
        pdbIdSet = set(pdbId)
        return infiles.filter(lambda t: str(t[0]) in pdbIdSet).map(_call_sequence_file)

    # Read in a random fraction of structures from a directory
    elif (pdbId == None and fraction != None):
        return infiles.sample(False, fraction, seed).map(_call_sequence_file)

    else:
        raise Exception("Inappropriate combination of parameters")


def read_mmtf_files(path):
    '''Read the specified PDB entries from a MMTF file

    Parameters
    ----------
    path : str
       Path to MMTF files

    Returns
    -------
    data
       structure data as keywork/value pairs
    '''

    if not os.path.exists(path):
        raise Exception("file path does not exist")

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    return sc.parallelize(_get_files(path)).map(_call_mmtf).filter(lambda t: t != None)


def download_mmtf_files(pdbIds, reduced=False):
    '''Download and reads the specified PDB entries using `MMTF web services <http://mmtf.rcsb.org/download.html>`_
    with either full or reduced format

    Parameters
    ----------
    path : str
       Path to PDB files
    reduced : bool
       flag to indicate reduced or full file format

    Returns
    -------
    data
       structure data as keywork/value pairs
    '''
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    return sc.parallelize(set(pdbIds)) \
             .map(lambda t: _get_structure(t, reduced)) \
             .filter(lambda t: t is not None)


def download_full_mmtf_files(pdbIds):
    '''Download and reads the specified PDB entries in full mmtf format using `MMTF web services
    <http://mmtf.rcsb.org/download.html>`_

    Parameters
    ----------
    path : str
       Path to PDB files

    Returns
    -------
    data
       structure data as keywork/value pairs
    '''
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    return sc.parallelize(set(pdbIds)) \
             .map(lambda t: _get_structure(t, False)) \
             .filter(lambda t: t is not None)


def download_reduced_mmtf_files(pdbIds):
    '''Download and reads the specified PDB entries in reduced mmtf format using `MMTF web services
    <http://mmtf.rcsb.org/download.html>`_

    Parameters
    ----------
    path : str
       Path to PDB files

    Returns
    -------
    data
       structure data as keywork/value pairs
    '''
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    
    return sc.parallelize(set(pdbIds)) \
             .map(lambda t: _get_structure(t, True)) \
             .filter(lambda t: t != None)


def _get_structure(pdbId, reduced):
    '''Download and decode a list of structure from a list of PDBid

    Parameters
    ----------
    pdbID : list
       List of structures to download

    Returns
    -------
    tuple
       pdbID and deccoder
    '''

    try:
        unpack = default_api.get_raw_data_from_url(pdbId, reduced)
        decoder = MmtfStructure(unpack)
        return (pdbId, decoder)
    except urllib.error.HTTPError:
        print(f"ERROR: {pdbId} is not a valid pdbId")


def _call_sequence_file(t):
    '''Call function for hadoop sequence files'''
    # TODO: check if all sequence files are gzipped
    data = default_api.ungzip_data(t[1])
    unpack = msgpack.unpackb(data.read(), raw=False)
    decoder = MmtfStructure(unpack)
    return (str(t[0]), decoder)


def _call_mmtf(f):
    '''Call function for mmtf files'''

    if ".mmtf.gz" in f:
        name = f.split('/')[-1].split('.')[0].upper()
        data = gzip.open(f, 'rb')
        unpack = msgpack.unpack(data, raw=False)
        decoder = MmtfStructure(unpack)
        return (name, decoder)

    elif ".mmtf" in f:
        name = f.split('/')[-1].split('.')[0].upper()
        unpack = msgpack.unpack(open(f, "rb"), raw=False)
        decoder = MmtfStructure(unpack)
        return (name, decoder)


def _get_files(user_path):
    '''Get List of files from path

    Parameters
    ----------
    user_path : str
       File path

    Returns
    -------
    list
       files in path
    '''
    files = []
    for dirpath, dirnames, filenames in walk(user_path):
        for f in filenames:
            if path.isdir(f):
                files += getFiles(f)
            else:
                files.append(dirpath + '/' + f)
    return files


def get_mmtf_full_path():
    '''Returns the path to the full MMTF-Hadoop sequence file.
    It looks for the environmental variable "MMTF_FULL", if not set, an error
    message will be shown.

    Returns
    -------
    str
       path to the mmtf_full directory
    '''

    if 'MMTF_FULL' in os.environ:
        print(
            f"Hadoop Sequence file path: MMTF_FULL={os.environ.get('MMTF_FULL')}")
        return os.environ.get("MMTF_FULL")
    else:
        raise EnvironmentError("Environmental variable 'MMTF_FULL not set'")


def get_mmtf_reduced_path():
    '''Returns the path to the reduced MMTF-Hadoop sequence file.
    It looks for the environmental variable "MMTF_REDUCED", if not set, an error
    message will be shown.

    Returns
    -------
    str
       path to the mmtf_reduced directory
    '''

    if 'MMTF_REDUCED' in os.environ:
        print(
            f"Hadoop Sequence file path: MMTF_REDUCED={os.environ.get('MMTF_REDUCED')}")
        return os.environ.get("MMTF_REDUCED")
    else:
        raise EnvironmentError("Environmental variable 'MMTF_REDUCED not set'")
