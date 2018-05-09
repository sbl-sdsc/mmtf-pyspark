#!/user/bin/env python
'''notFilter.py

This filter wraps another filter and negates its result

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"


class NotFilter(object):
    '''Constructor takes another filter as input

    Attributes
    ----------
    filter1 : filter
       first filter to be negated
    filter2 : filter
       second filter to be negated
    '''

    def __init__(self, filter_function):
        self.filter = filter_function

    def __call__(self, t):
        return not self.filter(t)
