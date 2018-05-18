#!/user/bin/env python
'''mmtfWriter.py

Encodes and write MMTF encoded structure data to a Hadoop Sequence File

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtf.api.mmtf_writer import MMTFEncoder
from mmtfPyspark.utils import MmtfStructure
from pyspark.sql import SparkSession
import gzip
import msgpack
import os
import base64


def write_sequence_file(path, structure, compressed=True):
    '''Encodes and writes MMTF encoded structure data to a Hadoop Sequnce File

    Parameters
    ----------
    path : str
       Path to Hadoop file directory)
    structure : tuple
       structure data to be written
    compress : bool
       if true, apply gzip compression
    '''
    # Can't apply first() function on list
    if type(structure.first()[1]) == MmtfStructure:
        structure = structure.map(lambda s: (s[0], s[1].set_alt_loc_list()))

    structure.map(lambda t: (t[0], _to_byte_array(t[1], compressed)))\
             .saveAsHadoopFile(path,
                               "org.apache.hadoop.mapred.SequenceFileOutputFormat",
                               "org.apache.hadoop.io.Text",
                               "org.apache.hadoop.io.BytesWritable")


def write_mmtf_files(path, structure):
    '''Encodes and writes MMTF encoded and gzipped structure data to individual .mmtf.gz files.

    Parameters
    ----------
    path : str
       Path to Hadoop file directory
    structure : tuple
       structure data to be written
    '''

    if path[-1] != "/":
        path = path + "/"

    if not os.path.exists(path):
        os.makedirs(path)

    structure = structure.map(lambda s: (s[0], s[1].set_alt_loc_list())) \
                         .map(lambda t: (t[0], _to_byte_array(t[1], False))) \
                         .foreach(lambda t: gzip.open(path + t[0] + '.mmtf.gz', mode='wb').write(t[1]))


def to_mmtf_base64(structure):
    '''Encodes a mmtfStructure to base64 byte array

    Parameters
    ----------
    structure : mmtfStructure
       structure to be encoded to base64 byte array

    Returns
    -------
    list
       base64 byte array
    '''

    byteArray = _to_byte_array(structure, compressed=False)
    return base64.b64encode(byteArray).decode()


def _to_byte_array(structure, compressed):
    '''Returns an MMTF-encoded byte array with optional gzip compression

    Returns
    -------
    list
       MMTF encoded and optionally gzipped structure data
    '''

    if type(structure) == MmtfStructure:
        if not structure.alt_loc_set:
            structure = structure.set_alt_loc_list()

    byte_array = bytearray(msgpack.packb(MMTFEncoder.encode_data(structure), use_bin_type = True))

    if compressed:
        return gzip.compress(byte_array)
    else:
        return byte_array
