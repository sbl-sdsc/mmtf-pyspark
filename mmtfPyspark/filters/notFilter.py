#!/user/bin/env python
'''
notFilter.py

This filter wraps another filter and negates its result

authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "done"
'''

class notFilter(object):
    '''
    Constructor takes another filter as input

    Attributes:
        filter1 (filter): first filter to be negated
        filter2 (filter): second filter to be negated
    '''

    def __init__(self, filter_function):
        self.filter = filter_function


    def __call__(self,t):
        return not self.filter(t)
