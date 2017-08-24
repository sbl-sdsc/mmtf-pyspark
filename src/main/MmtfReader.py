#!/usr/bin/env python
'''
MmtfSReader.py: Reads and decodes an MMTF Hadoop Sequence file.
(e.g. PDB ID) as the key and the MMTF StructureDataInterface as the value.

Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
'''
#TODO Docstring
#TODO Deflat Gzip try catch
#TODO

from mmtf.api.mmtf_reader import MMTFDecoder
from mmtf.api import default_api
from os import walk
from os import path
import msgpack


#Create variables
text = "org.apache.hadoop.io.Text"
byteWritable = "org.apache.hadoop.io.BytesWritable"

def call(t):
    data = default_api.ungzip_data(t[1])
    unpack = msgpack.unpackb(data.read())
    decoder = MMTFDecoder()
    decoder.decode_data(unpack)
    return (str(t[0]),decoder)


def call_mmtf(f):
    if ".mmtf.gz" in f:
        name = f.split('/')[-1].split('.')[0].upper()
        decoder = default_api.parse_gzip(f)
        return (name,decoder)

    elif ".mmtf" in f:
        name = f.split('/')[-1].split('.')[0].upper()
        decoder = default_api.parse(f)
        return (name,decoder)

        #else:
        #    return None

    #except Exception:
        #print("error reading file")
    #    return None


def getFiles(user_path):
    files = []
    for dirpath, dirnames, filenames in walk(user_path):
        for f in filenames:
            if path.isdir(f):
                files += getFiles(f)
            else:
                files.append(dirpath + '/' + f)
    return files


def getStructure(pdbId):
    unpack = default_api.get_raw_data_from_url(pdbId)
    decoder = MMTFDecoder()
    decoder.decode_data(unpack)
    return (pdbId,decoder)


#def read(path,sc):
#    infiles = sc.sequenceFile(path, text, byteWritable)
#    return infiles.count() #.map(call)

def readSequenceFile(path,sc, pdbId = None, fraction = None, seed = None):

    infiles = sc.sequenceFile(path, text, byteWritable)

    if (pdbId == None and fraction == None and seed == None):
        return infiles.map(call)

    elif(pdbId != None and fraction == None and seed == None):
        pdbIdSet = set(pdbId)
        return infiles.filter(lambda t: str(t[0]) in pdbIdSet).map(call)

    elif (fraction != None and seed != None):
        return infiles.sample(False, fraction, seed).map(call)
#Another read function that takes list of pdb as input


def readMmtfFiles(path, sc):
    return sc.parallelize(getFiles(path)).map(call_mmtf).filter(lambda t: t != None)



def downloadMmtfFiles(pdbIds, sc):

    return sc.parallelize(set(pdbIds)).map(lambda t: getStructure(t))

#if __name__ == "__main__":
#    read(path,sc)
