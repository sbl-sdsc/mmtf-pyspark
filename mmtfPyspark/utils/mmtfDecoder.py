'''mmtfDecoder.py

Provides efficient methods to decode mmtf structures
'''
__author__ = "Mars (Shih-Cheng) Huang, Peter W Rose"
__maintainer__ = "Peter W Rose"
__email__ = "pwrose.ucsd@gmail.com"
__version__ = "0.3.7"
__status__ = "experimental"

import numpy as np
from numba import jit

USE_NUMBA = True

#
# Byte arrays in message pack are in big endian format, e.g. >i4.
# Convert to little endian as expected by Python.


def get_value(input_data, field_name, required=False):
    if field_name in input_data:
        return input_data[field_name]
    elif required:
        raise Exception('ERROR: Invalid MMTF File, field: {} is missing!'.format(field_name))
    else:
        return None


def decode(input_data, field_name, required=False):
    if field_name in input_data:
        encoding = np.frombuffer(input_data[field_name][0:4], '>i4').byteswap().newbyteorder()[0]
        if encoding == 2:
            return decode_type_2(input_data, field_name)
        elif encoding == 4:
            return decode_type_4(input_data, field_name)
        elif encoding == 5:
            return decode_type_5(input_data, field_name)
        elif encoding == 6:
            return decode_type_6(input_data, field_name)
        elif encoding == 8:
            return decode_type_8(input_data, field_name)
        elif encoding == 9:
            return decode_type_9(input_data, field_name)
        elif encoding == 10:
            return decode_type_10(input_data, field_name)
        else:
            raise Exception('ERROR: MMTF encoding type not supported : {}!'.format(field_name))
    elif required:
        raise Exception('ERROR: Invalid MMTF File, field: {} is missing!'.format(field_name))
    else:
        return []


def decode_type_2(input_data, field_name):
    return np.frombuffer(input_data[field_name], '>i1', offset=12).byteswap().newbyteorder()


def decode_type_4(input_data, field_name):
    return np.frombuffer(input_data[field_name], '>i4', offset=12).byteswap().newbyteorder()


def decode_type_5(input_data, field_name):
        return np.frombuffer(input_data[field_name], 'S4', offset=12).astype(str)


def decode_type_6(input_data, field_name):
    length = np.frombuffer(input_data[field_name][4:8], '>i').byteswap().newbyteorder()[0]
    int_array = np.frombuffer(input_data[field_name], '>i4', offset=12).byteswap().newbyteorder()
    return run_length_decoder_ascii(int_array, length)


def decode_type_8(input_data, field_name):
    length = np.frombuffer(input_data[field_name][4:8], '>i').byteswap().newbyteorder()[0]
    int_array = np.frombuffer(input_data[field_name], '>i4', offset=12).byteswap().newbyteorder()
    if USE_NUMBA:
        return np.cumsum(run_length_decoder_jit(int_array, length)).astype(np.int32)
    else:
        return np.cumsum(run_length_decoder(int_array, length)).astype(np.int32)


def decode_type_9(input_data, field_name):
    length = np.frombuffer(input_data[field_name][4:8], '>i').byteswap().newbyteorder()[0]
    buffer = input_data[field_name]
    int_array = np.frombuffer(buffer, '>i4', offset=12).byteswap().newbyteorder()
    divisor = np.frombuffer(buffer[8:12], '>i').byteswap().newbyteorder()[0]
    if USE_NUMBA:
        return (run_length_decoder_jit(int_array, length) / divisor).astype(np.float32)
    else:
        return (run_length_decoder(int_array, length) / divisor).astype(np.float32)


def decode_type_10(input_data, field_name):
    buffer = input_data[field_name]
    #int_array = np.frombuffer(buffer[12:], '>i2').byteswap().newbyteorder()
    int_array = np.frombuffer(buffer, '>i2', offset=12).byteswap().newbyteorder()
    divisor = np.frombuffer(buffer[8:12], '>i').byteswap().newbyteorder()
    if USE_NUMBA:
        return (recursive_index_decode_jit(int_array, divisor)).astype(np.float32)
    else:
        return (recursive_index_decode(int_array, divisor)).astype(np.float32)


def run_length_decoder(in_array, n):
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
    x = np.full(n, np.nan)
    for l, h, v in zip(starts, ends, values):
        x[l:h] = v
    return x


@jit(nopython=True)
def run_length_decoder_jit(x, n):
    """Decodes a run length encoded array

    Parameters
    ----------
    x : encoded array of integers (value, repeat pairs)
    n : number of element in decoded array
    """
    y = np.empty(n)
    start = 0
    for i in range(0, x.shape[0] - 1, 2):
        end = x[i + 1] + start
        y[start:end] = x[i]
        start = end
    return y


def recursive_index_decode(int_array, divisor=1000):
    """Unpack an array of integers using recursive indexing.

    Parameters
    ----------
    int_array : list
       the input array of integers
    divisor : int
       the number used for decoding [1000]

    Returns
    -------
    numpy.array
       return the numpy.array of integers after recursive index decoding
    """
    maximum = 32767
    minimum = -32768
    out_arr = np.cumsum(int_array) / divisor
    return out_arr[(int_array != maximum) & (int_array != minimum)]


@jit(nopython=True)
def recursive_index_decode_jit(int_array, divisor=1000):
    """Unpack an array of integers using recursive indexing.

    Parameters
    ----------
    int_array : list
       the input array of integers
    divisor : int
       the number used for decoding [1000]

    Returns
    -------
    numpy.array
       return the numpy.array of integers after recursive index decoding
    """
    maximum = 32767
    minimum = -32768
    out_arr = np.cumsum(int_array) / divisor
    return out_arr[(int_array != maximum) & (int_array != minimum)]


def run_length_decoder_ascii(x, n):
    """Decodes a run length encoded array

    Parameters
    ----------
    x : encoded array of integers (value, repeat pairs)
    n : number of element in decoded array
    """
    # TODO initialize as str or np.object_ or default?
    y = np.empty(n, dtype=str)
    start = 0
    for i in range(0, x.shape[0] - 1, 2):
        end = x[i + 1] + start
        y[start:end] = chr(x[i])
        start = end
    return y

# def decode_entity_list(input_data):
#     """Convert byte strings to strings in the entity list.
#
#     Parameters
#     ----------
#     input_data : list
#        the list of entities
#
#     Returns
#     -------
#     list
#        decoded entity list
#     """
#     return [convert_entity(entry) for entry in input_data]
#
# TODO check if these methods are still required
# def decode_group_list(input_data):
#     """Convert byte strings to strings in the group map.
#
#     Parameters
#     ----------
#     input_data : list
#        the list of groups
#
#     Returns
#     -------
#     list
#        decoded group list
#     """
#     return [convert_group(entry) for entry in input_data]
#
#
# def convert_group(input_group):
#     """Convert an individual group from byte strings to regular strings.
#
#     Parameters
#     ----------
#     input_group : list
#        the list of input groups
#
#     Returns
#     -------
#     dict
#     """
#
#     output_group = {}
#     for key in input_group:
#         if key in [b'elementList', b'atomNameList']:
#             output_group[key.decode('ascii')] = [x.decode('ascii')
#                                                  for x in input_group[key]]
#         elif key in [b'chemCompType', b'groupName', b'singleLetterCode']:
#             output_group[key.decode(
#                 'ascii')] = input_group[key].decode('ascii')
#         else:
#             output_group[key.decode('ascii')] = input_group[key]
#     return output_group
#
#
# def convert_entity(input_entity):
#     """Convert an individual entity from byte strings to regular strings
#
#     Parameters
#     ----------
#     input_entity : list
#        entities to decode
#
#     Returns
#     -------
#     dict
#        decoded entity
#     """
#     output_entity = {}
#     for key in input_entity:
#         if key in [b'description', b'type', b'sequence']:
#             output_entity[key.decode('ascii')] = input_entity[key].decode('ascii')
#         else:
#             output_entity[key.decode('ascii')] = input_entity[key]
#     return output_entity
