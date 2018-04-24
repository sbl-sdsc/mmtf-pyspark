#!/usr/bin/env python
'''mmtfReader.py: Methods for reading and downloading structures in MMTF file
formats. The data are returned as a PythonRDD with the structure id (e.g. PDB ID)
as the key and the structural data as the value.

Supported operations and file formats:
    - Read directory of MMTF-Hadoop sequence files in full and reduced representation
    - Download MMTF full and reduced representations using web service (mmtf.rcsb.org)
    - Read directory of MMTF files (.mmtf, mmtf.gz)

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com"
    __version__ = "0.2.0"
    __status__ = "Done"
'''

import os
import msgpack
import gzip
from mmtfPyspark.utils import MmtfStructure
from mmtf.api import default_api
from os import path, walk

text = "org.apache.hadoop.io.Text"
byteWritable = "org.apache.hadoop.io.BytesWritable"


def read_full_sequence_file(sc, pdbId=None, fraction=None, seed=123):
    '''Reads a MMTF-Hadoop Sequence file using the default file location.
    The default file location is determined by {mmtfReader.get_mmtf_full_path}

    Downloads
    ---------
        To download mmtf files: {https://mmtf.rcsb.org/download.htm}

    Attributes
    ----------
        sc (Spark Context):
        pdbID (list(str)): List of structures to read
        fraction (float): fraction of structure to read
        seed (int): random seed
    '''
    return read_sequence_file(get_mmtf_full_path(), sc, pdbId, fraction, seed)


def read_reduced_sequence_file(sc, pdbId=None, fraction=None, seed=123):
    '''Reads a MMTF-Hadoop Sequence file using the default file location.
    The default file location is determined by {mmtfReader.get_mmtf_reduced_path}

    Downloads
    ---------
        To download mmtf files: {https://mmtf.rcsb.org/download.htm}

    Attributes
    ----------
        sc (Spark Context):
        pdbID (list(str)): List of structures to read
        fraction (float): fraction of structure to read
        seed (int): random seed
    '''
    return read_sequence_file(get_mmtf_reduced_path(), sc, pdbId, fraction, seed)


def read_sequence_file(path, sc, pdbId=None, fraction=None, seed=123):
    '''Reads an MMTF Hadoop Sequence File. Can read all files from path,
    randomly rample a fraction, or a subset based on input list.
    See <a href="http://mmtf.rcsb.org/download.html"> for file download information</a>

    Attributes
    ----------
        path (str): path to file directory
        sc (Spark Context):
        pdbID (list(str)): List of structures to read
        fraction (float): fraction of structure to read
        seed (int): random seed
    '''

    if not os.path.exists(path):
        raise Exception("file path does not exist")

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


def read_mmtf_files(path, sc):
    '''Read the specified PDB entries from a MMTF file

    Attributes
    ----------
        path (str): Path to MMTF files
        sc (Spark context)

    Returns
    -------
        structure data as keywork/value pairs
    '''

    if not os.path.exists(path):
        raise Exception("file path does not exist")

    return sc.parallelize(_get_files(path)).map(_call_mmtf).filter(lambda t: t != None)


def download_mmtf_files(pdbIds, sc, reduced=False):
    '''Download and reads the specified PDB entries using
    <a href="http://mmtf.rcsb.org/download.html">MMTF web services</a>.
    with either full or reduced format

    Attributes
    ----------
        path (str): Path to PDB files
        sc (Spark context)
        reduced (bool): flag to indicate reduced or full file format

    Returns
    -------
        structure data as keywork/value pairs
    '''

    return sc.parallelize(set(pdbIds)).map(lambda t: _get_structure(t, reduced))


def download_full_mmtf_files(pdbIds, sc):
    '''Download and reads the specified PDB entries in full mmtf format using
    <a href="http://mmtf.rcsb.org/download.html">MMTF web services</a>.

    Attributes
    ----------
        path (str): Path to PDB files
        sc (Spark context)

    Returns
    -------
        structure data as keywork/value pairs
    '''

    return sc.parallelize(set(pdbIds)).map(lambda t: _get_structure(t, False))


def download_reduced_mmtf_files(pdbIds, sc):
    '''Download and reads the specified PDB entries in reduced mmtf format using
    <a href="http://mmtf.rcsb.org/download.html">MMTF web services</a>.

    Attributes
    ----------
        path (str): Path to PDB files
        sc (Spark context)

    Returns
    -------
        structure data as keywork/value pairs
    '''

    return sc.parallelize(set(pdbIds)).map(lambda t: _get_structure(t, True))


def _get_structure(pdbId, reduced):
    '''Download and decode a list of structure from a list of PDBid

    Attributes
    ----------
        pdbID (List(str)): List of structures to download

    Returns
    -------
        tuble of pdbID and deccoder
    '''

    unpack = default_api.get_raw_data_from_url(pdbId, reduced)
    decoder = MmtfStructure(unpack)
    return (pdbId, decoder)


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

    Attributes
    ----------
        user_path (str): File path

    Returns
    -------
        files (List(str)): list of files in path
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
        path to the mmtf_reduced directory
    '''

    if 'MMTF_REDUCED' in os.environ:
        print(
            f"Hadoop Sequence file path: MMTF_REDUCED={os.environ.get('MMTF_REDUCED')}")
        return os.environ.get("MMTF_REDUCED")
    else:
        raise EnvironmentError("Environmental variable 'MMTF_REDUCED not set'")
