#!/usr/bin/env python
'''
MmtfSequenceFileReader.py: Reads and decodes an MMTF Hadoop Sequence file.
(e.g. PDB ID) as the key and the MMTF StructureDataInterface as the value.

Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
TODO:
'''
from mmtf.api.mmtf_reader import MMTFDecoder
from mmtf.api import default_api
import msgpack

# TODO Docstring
# TODO Update with:
#       - Download from internet instead of local
#       - Generate fraction of files
#       - More info (Spark/IO/MMTFReader)

#Create variables
text = "org.apache.hadoop.io.Text"
byteWritable = "org.apache.hadoop.io.BytesWritable"

def call(t):
    data = default_api.ungzip_data(t[1])
    unpack = msgpack.unpackb(data.read())
    decoder = MMTFDecoder()
    decoder.decode_data(unpack)
    return (str(t[0]),decoder)

def read(path,sc):
    infiles = sc.sequenceFile(path, text, byteWritable)
    return infiles.map(call)

#Another read function that takes list of pdb as input

if __name__ == "__main__":
    read(path,sc)
