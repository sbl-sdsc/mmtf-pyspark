#!/user/bin/env python
'''
MMTFWriter.py

Encodes and write MMTF encoded structure data to a Hadoop Sequence File

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from mmtf.api.mmtf_writer import MMTFEncoder
import msgpack

def writeSequenceFile(path, sc, structure, compressed = True):
    '''
    Encodes and writes MMTF encoded structure data to a Hadoop Sequnce File

    Attributes:
        path (str): Path to Hadoop file directory)
        sc (Spark context)
        structure (tuple): structure data to be written
        compress (bool): if true, apply gzip compression
    '''

    structure.map(lambda t: (t[0], toByteArray(t[1], compressed))).saveAsHadoopFile(path,
                               "org.apache.hadoop.mapred.SequenceFileOutputFormat",
                               "org.apache.hadoop.io.Text",
                               "org.apache.hadoop.io.BytesWritable")

def toByteArray(structure, compressed):
    '''
    Returns an MMTF-encoded byte array with optional gzip compression

    Returns:
        MMTF encoded and optionally gzipped structure data
    '''
    mmtf_dict = MMTFEncoder.encode_data(structure)
    #return bytearray().extend(map(ord,[[k,v] for k,v in mmtf_dict.items()]))
    return bytearray(msgpack.packb(MMTFEncoder.encode_data(structure)))
