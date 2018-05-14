'''mmtfDecoder.py

Provides efficient methods to decode mmtf structures
'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

import numpy as np


def run_length_decoder_numpy(in_array):
    """Decodes a run length encoded array

    Parameters
    ----------
    in_array : list
       the input list to apply run length decoder on
    """
    lengths = np.array(in_array[1::2])
    values = np.array(in_array[0::2])
    starts = np.insert(np.array([0]), 1, np.cumsum(lengths))[:-1]
    ends = starts + lengths
    n = ends[-1]
    x = np.full(n, np.nan)
    for l, h, v in zip(starts, ends, values):
        x[l:h] = v
    return x


def recursive_index_decode(int_array, decode_num=1000):
    """Unpack an array of integers using recursive indexing.

    Parameters
    ----------
    int_array : list
       the input array of integers
    decode_num : int
       the number used for decoding [1000]

    Returns
    -------
    numpy.array
       return the numpy.array of integers after recursive index decoding
    """
    maximum = 32767
    minimum = -32768
    out_arr = np.cumsum(int_array) / decode_num
    return out_arr[(int_array != maximum) & (int_array != minimum)]


def decode_entity_list(input_data):
    """Convert byte strings to strings in the entity list.

    Parameters
    ----------
    input_data : list
       the list of entities

    Returns
    -------
    list
       decoded entity list
    """
    return [convert_entity(entry) for entry in input_data]


def decode_group_list(input_data):
    """Convert byte strings to strings in the group map.

    Parameters
    ----------
    input_data : list
       the list of groups

    Returns
    -------
    list
       decoded group list
    """
    return [convert_group(entry) for entry in input_data]


def convert_group(input_group):
    """Convert an individual group from byte strings to regula strings.

    Parameters
    ----------
    input_group : list
       the list of input groups

    Returns
    -------
    dict
    """

    output_group = {}
    for key in input_group:
        if key in [b'elementList', b'atomNameList']:
            output_group[key.decode('ascii')] = [x.decode('ascii')
                                                 for x in input_group[key]]
        elif key in [b'chemCompType', b'groupName', b'singleLetterCode']:
            output_group[key.decode(
                'ascii')] = input_group[key].decode('ascii')
        else:
            output_group[key.decode('ascii')] = input_group[key]
    return output_group


def convert_entity(input_entity):
    """Convert an individual entity from byte strings to regular strings

    Parameters
    ----------
    input_entity : list
       entities to decode

    Returns
    -------
    dict
       decoded entity
    """
    output_entity = {}
    for key in input_entity:
        if key in [b'description', b'type', b'sequence']:
            output_entity[key.decode('ascii')] = input_entity[key].decode('ascii')
        else:
            output_entity[key.decode('ascii')] = input_entity[key]
    return output_entity
