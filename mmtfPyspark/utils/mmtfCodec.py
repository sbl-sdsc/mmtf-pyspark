#!/usr/bin/env python
# coding: utf-8

import msgpack
import struct
import numpy as np
from numba import njit


def decode_array(input_array):
    """Parse the header of an input byte array and then decode using the input array,
    the codec and the appropirate parameter.
    :param input_array: the array to be decoded
    :return the decoded array"""
    codec, length, param, in_array = parse_header(input_array)
    decode_func = codec_dict.get(codec)
    return decode_func.decode(in_array, length, param)
#    return codec_dict[codec].decode(in_array, length, param)




@njit
def f2id_numba(x, multiplier):
    y = np.empty(x.shape[0], dtype=np.int32)
    y[0] = round(x[0] * multiplier)

    for i in range(1, x.shape[0]):
        y[i] = round((x[i] - x[i - 1]) * multiplier)

    return y


# In[3]:


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


# # In[4]:


@njit
def cum_sum(x):
    y = np.empty(x.shape[0], dtype=np.int32)
    y[0] = x[0]
    for i in range(1, x.shape[0]):
        y[i] = x[i - 1] + x[i]

    return y


# In[5]:


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
    #y = np.cumsum(x) / divisor
    print("ri_decode", divisor)
    y = cum_sum(x) / divisor
    return y[(x != maximum) & (x != minimum)]


# In[6]:


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


# In[7]:


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

# TODO use np.unique for run-length encoding?
# >>> a = np.array([1, 2, 6, 4, 2, 3, 2])
# >>> u, indices = np.unique(a, return_inverse=True)
# >>> u
# array([1, 2, 3, 4, 6])
# >>> indices
# array([0, 1, 4, 3, 1, 2, 1])
# >>> u[indices]
# array([1, 2, 6, 4, 2, 3, 2])
# In[8]:


@njit
def delta(x):
    y = np.empty(x.shape[0], dtype=np.int32)
    y[0] = x[0]
    for i in range(1, x.shape[0]):
        y[i] = x[i] - x[i - 1]

    return y


# In[9]:


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


# In[10]:


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


# In[11]:


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


# In[12]:


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


# In[13]:


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


# In[14]:


class Type10:
    """Covert an array of floats to integers, perform delta
    encoding and then use recursive indexing to store as 2
    byte integers in a byte array."""

    @staticmethod
    def decode(in_array, length, param):
        #int_array = np.frombuffer(in_array, '>i2').byteswap().newbyteorder()
        #return ri_decode(int_array, param).astype(np.float32)
        return np.empty(length, np.float32)

    @staticmethod
    def encode(in_array, param):
        y = ri_encode(f2id_numba(in_array, param))
        return y.byteswap().newbyteorder().tobytes()

# In[16]:


class Type9:
    """Covert an array of floats to integers, perform delta
    encoding and then use recursive indexing to store as 2
    byte integers in a byte array."""

    @staticmethod
    def decode(in_array, length, param):
        int_array = np.frombuffer(in_array, '>i4').byteswap().newbyteorder()
        return run_length_div_decode(int_array, length, param)

    @staticmethod
    def encode(in_array, param):
        y = run_length_div_encode(in_array, param)
        return y.byteswap().newbyteorder().tobytes()


# In[17]:


class Type8:
    """Covert an array of floats to integers, perform delta
    encoding and then use recursive indexing to store as 2
    byte integers in a byte array."""

    @staticmethod
    def decode(in_array, length, param):
        int_array = np.frombuffer(in_array, '>i4').byteswap().newbyteorder()
        return np.cumsum(run_length_decode(int_array, length))

    @staticmethod
    def encode(in_array, param):
        y = run_length_encode(delta(in_array))
        return y.byteswap().newbyteorder().tobytes()


# In[18]:


class Type6:
    """Covert an array of floats to integers, perform delta
    encoding and then use recursive indexing to store as 2
    byte integers in a byte array."""

    @staticmethod
    def decode(in_array, length, param):
        int_array = np.frombuffer(in_array, '>i4').byteswap().newbyteorder()
        return run_length_decoder_ascii(int_array, length)

    @staticmethod
    def encode(in_array, param):
        y = run_length_encode_ascii(in_array)
        # y = run_length_encode_asc(in_array)
        return y.byteswap().newbyteorder().tobytes()


# In[19]:


class Type5:
    """Covert an array of floats to integers, perform delta
    encoding and then use recursive indexing to store as 2
    byte integers in a byte array."""

    @staticmethod
    def decode(in_array, length, param):
        return np.frombuffer(in_array, 'S4').astype(str)

    @staticmethod
    def encode(in_array, param):
        return encode_chain_list(in_array)


# In[20]:


class Type4:
    """Covert an array of floats to integers, perform delta
    encoding and then use recursive indexing to store as 2
    byte integers in a byte array."""

    @staticmethod
    def decode(in_array, length, param):
        return np.frombuffer(in_array, '>i4').byteswap().newbyteorder()

    @staticmethod
    def encode(in_array, param):
        return in_array.astype(np.int32).byteswap().newbyteorder().tobytes()


# In[21]:

class Type2:
    """Covert an array of floats to integers, perform delta
    encoding and then use recursive indexing to store as 2
    byte integers in a byte array."""

    @staticmethod
    def decode(in_array, length, param):
        return np.frombuffer(in_array, '>i1')

    @staticmethod
    def encode(in_array, param):
        return in_array.astype(np.int8).tobytes()


codec_dict = {2: Type2,
              10: Type10,
              9: Type9,
              8: Type8,
              6: Type6,
              5: Type5,
              4: Type4}


def parse_header(input_array):
    """Parse the header and return it along with the input array minus the header.
    :param input_array the array to parse
    :return the codec, the length of the decoded array, the parameter and the remainder
    of the array"""
    codec = struct.unpack(">i", input_array[0:4])[0].byteswap().newbyteorder()
    length = struct.unpack(">i", input_array[4:8])[0].byteswap().newbyteorder()
    param = struct.unpack(">i", input_array[8:12])[0].byteswap().newbyteorder()
    return codec, length, param, input_array[12:]


# In[24]:


def add_header(input_array, codec, length, param):
    """Add the header to the appropriate array.
    :param the encoded array to add the header to
    :param the codec being used
    :param the length of the decoded array
    :param the parameter to add to the header
    :return the prepended encoded byte array"""
    return struct.pack(">i", codec) + struct.pack(">i", length) + struct.pack(">i", param) + input_array


# In[25]:




# In[26]:


def encode_array(input_array, codec, param):
    """Encode the array using the method and then add the header to this array.
    :param input_array: the array to be encoded
    :param codec: the integer index of the codec to use
    :param param: the integer parameter to use in the function
    :return an array with the header added to the fornt"""
    return add_header(codec_dict[codec].encode(input_array, param), codec, len(input_array), param)

def get_msgpack(data):
    """Get the msgpack of the encoded data."""
    return msgpack.packb(data, use_bin_type=True)


def write_file(file_path, data):
    with open(file_path, "wb") as out_f:
        out_f.write(data)

