#!/usr/bin/env python
# coding: utf-8

import msgpack
import struct
import numpy as np
from numba import njit


class Codec(object):

    def decode_array(self, input_array):
        """Parse the header of an input byte array and then decode using the input array,
    the codec and the appropirate parameter.
    :param input_array: the array to be decoded
    :return the decoded array"""

        codec, length, param, in_array = parse_header(input_array)
        decode_func = getattr(self, "decode" + str(codec))
        return decode_func(in_array, length, param)

    def decode2(self, in_array, length, param):
        return np.frombuffer(in_array, '>i1')

    def encode2(self, in_array, param):
        return in_array.astype(np.int8).tobytes()

    def decode4(self, in_array, length, param):
        return np.frombuffer(in_array, '>i4').byteswap().newbyteorder()

    def encode4(self, in_array, param):
        return in_array.astype(np.int32).byteswap().newbyteorder().tobytes()

    def decode5(self, in_array, length, param):
        return np.frombuffer(in_array, 'S4').astype(str)

    def encode5(self, in_array, param):
        return encode_chain_list(in_array)

    def decode6(self, in_array, length, param):
        int_array = np.frombuffer(in_array, '>i4').byteswap().newbyteorder()
        return run_length_decoder_ascii(int_array, length)

    def encode6(self, in_array, param):
        y = run_length_encode_ascii(in_array)
        return y.byteswap().newbyteorder().tobytes()

    def decode6(self, in_array, length, param):
        int_array = np.frombuffer(in_array, '>i4').byteswap().newbyteorder()
        return run_length_decoder_ascii(int_array, length)

    def encode6(self, in_array, param):
        y = run_length_encode_ascii(in_array)
        return y.byteswap().newbyteorder().tobytes()

    def decode8(self, in_array, length, param):
        int_array = np.frombuffer(in_array, '>i4').byteswap().newbyteorder()
        return np.cumsum(run_length_decode(int_array, length))

    def encode8(self, in_array, param):
        y = run_length_encode(delta(in_array))
        return y.byteswap().newbyteorder().tobytes()

    def decode9(self, in_array, length, param):
        int_array = np.frombuffer(in_array, '>i4').byteswap().newbyteorder()
        return run_length_div_decode(int_array, length, param)

    def encode9(self, in_array, param):
        y = run_length_div_encode(in_array, param)
        return y.byteswap().newbyteorder().tobytes()

    def decode10(self, in_array, length, param):
        int_array = np.frombuffer(in_array, '>i2').byteswap().newbyteorder()
        return ri_decode(int_array, param).astype(np.float32)

    def encode10(self, in_array, param):
        y = ri_encode(f2id_numba(in_array, param))
        return y.byteswap().newbyteorder().tobytes()


@njit
def f2id_numba(x, multiplier):
    y = np.empty(x.shape[0], dtype=np.int32)
    y[0] = round(x[0] * multiplier)

    for i in range(1, x.shape[0]):
        y[i] = round((x[i] - x[i - 1]) * multiplier)

    return y


@njit
def ri_encode(int_array, max=32767, min=-32768):
    """Pack an integer array using recursive indexing.
    :param int_array: the input array of integers
    :param max: the maximum integer size
    :param min: the minimum integer size
    :return the array of integers after recursive index encoding"""
    # TODO check if any value exceeds min/max -> skip this step?
    # TODO optimize speed, e.g. use mod to find #repeats
    out_arr = np.empty(int_array.shape[0] * 2, dtype=np.int16)
    i = 0
    for curr in int_array:
        if curr >= 0:
            while curr >= max:
                out_arr[i] = max
                i += 1
                curr -= max
        else:
            while curr <= min:
                out_arr[i] = min
                i += 1
                curr -= min
        out_arr[i] = curr
        i += 1
    return out_arr[:i]


@njit
def cum_sum(x):
    y = np.empty(x.shape[0], dtype=np.int32)
    y[0] = x[0]
    for i in range(1, x.shape[0]):
        y[i] = x[i - 1] + x[i]

    return y


@njit
def ri_decode(x, divisor):
    """Unpack an array of integers using recursive indexing.

    Parameters
    ----------
    x : list
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
    y = np.cumsum(x) / divisor
    #y = cum_sum(x) / divisor
    return y[(x != maximum) & (x != minimum)]


@njit
def run_length_div_decode(x, n, divisor):
    """Decodes a run length encoded array and scales/converts integer values to float

    Parameters
    ----------
    x : encoded array of integers (value, repeat pairs)
    n : number of element in decoded array
    """
    y = np.empty(n, dtype=np.float32)
    start = 0
    for i in range(0, x.shape[0] - 1, 2):
        end = x[i + 1] + start
        y[start:end] = x[i] / divisor
        start = end
    return y


@njit
def run_length_div_encode(x, divisor):
    y = np.empty(x.shape[0] * 2, dtype=np.int32)
    v = x[0]
    length = 0
    count = 0
    for i in x:
        if i == v:
            length += 1
        else:
            y[count] = round(v * divisor)
            count += 1
            y[count] = length
            count += 1
            v = i
            length = 1

    y[count] = round(v * divisor)
    count += 1
    y[count] = length

    return y[:count + 1]


@njit
def delta(x):
    y = np.empty(x.shape[0], dtype=np.int32)
    y[0] = x[0]
    for i in range(1, x.shape[0]):
        y[i] = x[i] - x[i - 1]

    return y


@njit
def run_length_decode(x, n):
    """Decodes a run length encoded array

    Parameters
    ----------
    x : encoded array of integers (value, repeat pairs)
    n : number of element in decoded array
    """
    y = np.empty(n, dtype=np.int32)
    start = 0
    for i in range(0, x.shape[0] - 1, 2):
        end = x[i + 1] + start
        y[start:end] = x[i]
        start = end
    return y


@njit
def run_length_encode(x):
    y = np.empty(x.shape[0] * 2, dtype=np.int32)
    v = x[0]
    length = 0
    count = 0
    for i in x:
        if i == v:
            length += 1
        else:
            y[count] = v
            count += 1
            y[count] = length
            count += 1
            v = i
            length = 1

    y[count] = v
    count += 1
    y[count] = length

    return y[:count + 1]


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


def run_length_encode_ascii(x):
    y = np.empty(x.shape[0] * 2, dtype=np.int32)
    v = x[0]
    length = 0
    count = 0
    for i in x:
        if i == v:
            length += 1
        else:
            y[count] = ord(v)
            count += 1
            y[count] = length
            count += 1
            v = i
            length = 1

    y[count] = ord(v)
    count += 1
    y[count] = length

    return y[:count + 1]

NULL_BYTE = '\x00'
nb = NULL_BYTE.encode('ascii')
CHAIN_LEN = 4


# TODO optimize this method
def encode_chain_list(in_strings):
    """Convert a list of strings to a list of byte arrays.
    :param in_strings: the input strings
    :return the encoded list of byte arrays"""
    out_bytes = b""
    for in_s in in_strings:
        out_bytes += in_s.encode('ascii')
        for i in range(CHAIN_LEN - len(in_s)):
            out_bytes += nb
    return out_bytes


def parse_header(input_array):
    """Parse the header and return it along with the input array minus the header.
    :param input_array the array to parse
    :return the codec, the length of the decoded array, the parameter and the remainder
    of the array"""
    codec = struct.unpack(">i", input_array[0:4])[0]
    length = struct.unpack(">i", input_array[4:8])[0]
    param = struct.unpack(">i", input_array[8:12])[0]
    print("parse_header", codec, length, param)
    return codec, length, param, input_array[12:]


def add_header(input_array, codec, length, param):
    """Add the header to the appropriate array.
    :param the encoded array to add the header to
    :param the codec being used
    :param the length of the decoded array
    :param the parameter to add to the header
    :return the prepended encoded byte array"""
    return struct.pack(">i", codec) + struct.pack(">i", length) + struct.pack(">i", param) + input_array
