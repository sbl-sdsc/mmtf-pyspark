#!/usr/bin/env python
'''
MmtfSReader.py: Reads and decodes an MMTF Hadoop Sequence file.
(e.g. PDB ID) as the key and the MMTF StructureDataInterface as the value.

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "debug"
'''

from mmtf.api.mmtf_reader import MMTFDecoder
from mmtf.api import default_api
try:
    from src.main.inputFunction import biopythonInputFunction
except ModuleNotFoundError:
    from ..inputFunction import biopythonInputFunction
from Bio.PDB import PDBParser, MMCIFParser, FastMMCIFParser
from mmtf import MMTFEncoder
from mmtf.api.default_api import pass_data_on
from os import walk
from os import path
from .mmtfStructure import mmtfStructure
import msgpack
import gzip

text = "org.apache.hadoop.io.Text"
byteWritable = "org.apache.hadoop.io.BytesWritable"

def call_sequence_file(t):
    '''
    Call function for hadoop sequence files
    '''
    unpack = msgpack.unpackb(t[1])
    decoder = mmtfStructure(unpack)
    #decoder = MMTFDecoder()
    #decoder.decode_data(unpack)
    return (str(t[0]), decoder)


def call_sequence_file_gzip(t):
    '''
    Call function for hadoop sequence files
    '''
    data = default_api.ungzip_data(t[1])
    unpack = msgpack.unpackb(data.read())
    decoder = mmtfStructure(unpack)
    return (str(t[0]), decoder)

def call_mmtf(f):
    '''
    Call function for mmtf files
    '''

    if ".mmtf.gz" in f:
        name = f.split('/')[-1].split('.')[0].upper()
        decoder = default_api.parse_gzip(f)
        return (name, decoder)

    elif ".mmtf" in f:
        name = f.split('/')[-1].split('.')[0].upper()
        decoder = default_api.parse(f)
        return (name, decoder)

    else:
        raise Exception("File format error")


def call_pdb(f):
    '''
    Call function for pdb files
    '''

    if (".pdb" or ".ent") in f:
        print(f)
        name = f.split('/')[-1].split('.')[0].upper()
        # Open gz files
        if ".gz" in f:
            f = gzip.open(f, 'rt')
        parser = PDBParser()
        structure = parser.get_structure(name, f)
        mmtf_encoder = MMTFEncoder()
        pass_data_on(input_data=structure, input_function=biopythonInputFunction,
                     output_data=mmtf_encoder)
        return (name, mmtf_encoder)

    else:
        raise Exception("File format error")


def call_mmcif(f):
    '''
    Call function for mmcif files
    '''

    if (".cif") in f:
        name = f.split('/')[-1].split('.')[0].upper()
        # Open gz files
        if ".gz" in f:
            f = gzip.open(f, 'rt')
        parser = MMCIFParser()
        structure = parser.get_structure(name, f)
        mmtf_encoder = MMTFEncoder()
        pass_data_on(input_data=structure, input_function=biopythonInputFunction,
                     output_data=mmtf_encoder)
        return (name, mmtf_encoder)

    else:
        raise Exception("File format error")


def call_fast_mmcif(f):
    '''
    Call function for mmcifr files (Using Fast Parser)
    '''

    if (".cif") in f:
        name = f.split('/')[-1].split('.')[0].upper()
        # Open gz files
        if ".gz" in f:
            f = gzip.open(f, 'rt')
        parser = FastMMCIFParser()
        structure = parser.get_structure(name, f)
        mmtf_encoder = MMTFEncoder()
        pass_data_on(input_data=structure, input_function=biopythonInputFunction,
                     output_data=mmtf_encoder)
        return (name, mmtf_encoder)

    else:
        raise Exception("File format error")


def getFiles(user_path):
    '''
    Get List of files from path

    Attributes:
        user_path (str): File path
    Return:
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


def getStructure(pdbId):
    '''
    Download and decode a list of structure from a list of PDBid

    Attributes:
        pdbID (List(str)): List of structures to download
    Return:
        tuble of pdbID and deccoder
    '''

    unpack = default_api.get_raw_data_from_url(pdbId)
    decoder = mmtfStructure(unpack)
    return (pdbId, decoder)


def readSequenceFile(path, sc, pdbId=None, fraction=None, seed=None, gz = True):
    '''
    Reads an MMTF Hadoop Sequence File. Can read all files from path,
    randomly rample a fraction, or a subset based on input list.
    See <a href="http://mmtf.rcsb.org/download.html"> for file download information</a>

    Attributes:
        path (str): path to file directory
        sc (Spark Context):
        pdbID (list(str)): List of structures to read
        fraction (float): fraction of structure to read
        seed (int): random seed
    '''
    if gz:
        call = call_sequence_file_gzip
    else:
        call = call_sequence_file

    infiles = sc.sequenceFile(path, text, byteWritable)

    if (pdbId == None and fraction == None and seed == None):
        return infiles.map(call)

    elif(pdbId != None and fraction == None and seed == None):
        pdbIdSet = set(pdbId)
        return infiles.filter(lambda t: str(t[0]) in pdbIdSet).map(call)

    elif (fraction != None and seed != None):
        return infiles.sample(False, fraction, seed).map(call)
    else:
        raise Exception("Inappropriate combination of parameters")


def readMmtfFiles(path, sc):
    '''
    Read the specified PDB entries from a MMTF file

    Attributes:
        path (str): Path to MMTF files
        sc (Spark context)

    Return:
        structure data as keywork/value pairs
    '''

    return sc.parallelize(getFiles(path)).map(call_mmtf).filter(lambda t: t != None)


def readPDBFiles(path, sc):
    '''
    Read the specified PDB entries from a PDB file

    Attributes:
        path (str): Path to PDB files
        sc (Spark context)

    Return:
        structure data as keywork/value pairs
    '''

    return sc.parallelize(getFiles(path)).map(call_pdb).filter(lambda t: t != None)


def readMmcifFiles(path, sc, fast=False):
    '''
    Read the specified PDB entries from a MMcif file

    Attributes:
        path (str): Path to MMcif files
        sc (Spark context)

    Return:
        structure data as keywork/value pairs
    '''

    if fast:
        return sc.parallelize(getFiles(path)).map(call_fast_mmcif).filter(lambda t: t != None)
    else:
        return sc.parallelize(getFiles(path)).map(call_mmcif).filter(lambda t: t != None)


def downloadMmtfFiles(pdbIds, sc):
    '''
    Download and reads the specified PDB entries using <a href="http://mmtf.rcsb.org/download.html">MMTF web services</a>.

    Attributes:
        path (str): Path to PDB files
        sc (Spark context)

    Return:
        structure data as keywork/value pairs
    '''

    return sc.parallelize(set(pdbIds)).map(lambda t: getStructure(t))
