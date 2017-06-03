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


def getStructure(pdbId):
    unpack = default_api.get_raw_data_from_url(pdbId)
    decoder = MMTFDecoder()
    decoder.decode_data(unpack)
    return (pdbId,decoder)


#def read(path,sc):
#    infiles = sc.sequenceFile(path, text, byteWritable)
#    return infiles.count() #.map(call)

# TODO Check logic flow
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


def downloadMmtfFiles(pdbIds, sc):
    return sc.parallelize(set(pdbIds)).map(lambda t: getStructure(t))

#if __name__ == "__main__":
#    read(path,sc)
