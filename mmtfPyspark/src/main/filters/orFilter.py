#!/user/bin/env python
'''
orFilter.py

This filter wraps two filter and returns true if one of the filters passes

authorship information:
    __author__ = "mars huang"
    __maintainer__ = "mars huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "done"
'''

class orFilter(object):
    '''
    Constructor takes another filter as input

    Attributes:
        filter1 (filter): first filter to be negated
        filter2 (filter): second filter to be negated


    '''

    def __init__(self, filter1, filter2):
        self.filter1 = filter1
        self.filter2 = filter2


    def __call__(self,t):
        return self.filter1(t) or self.filter2(t)
