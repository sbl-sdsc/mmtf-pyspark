'''mmtfDecoder.py

Provides efficient methods to decode mmtf structures

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com"
    __version__ = "0.2.0"
    __status__ = "done"
'''

import numpy as np


def run_length_decoder_numpy(in_array):
    """Decodes a run length encoded array

    Attributes
    ----------
        in_array (list): the input list to apply run length decoder on
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

    Attribute
    ---------
        int_array (list): the input array of integers
        decode_num (int): the number used for decoding [1000]

    Returns
    -------
        return the array of integers after recursive index decoding
    """

    maximum = 32767
    minimum = -32768
    out_arr = np.cumsum(int_array) / decode_num
    return out_arr[(int_array != maximum) & (int_array != minimum)]


def decode_entity_list(input_data):
    """Convert byte strings to strings in the entity list.

    Attributes
    ----------
        input_data (list): the list of entities

    Returns
    -------
        return the decoded entity list
    """

    out_data = []
    for entry in input_data:
        out_data.append(convert_entity(entry))
    return out_data


def decode_group_list(input_data):
    """Convert byte strings to strings in the group map.

    Attributes
    ----------
        input_data (list): the list of groups

    Returns
    -------
        return the decoded group list
    """

    out_data = []
    for entry in input_data:
        out_data.append(convert_group(entry))
    return out_data


def convert_group(input_group):
    """Convert an individual group from byte strings to regula strings.

    Attributes
    ----------
        input_group (list): the list of input groups

    Returns
    -------
        return the decoded group
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

    Attributes
    ----------
        input_entity (list): the list of entities to decode

    Returns
    -------
        return the decoded entity
    """

    output_entity = {}
    for key in input_entity:
        if key in [b'description', b'type', b'sequence']:
            output_entity[key.decode(
                'ascii')] = input_entity[key].decode('ascii')
        else:
            output_entity[key.decode('ascii')] = input_entity[key]
    return output_entity
