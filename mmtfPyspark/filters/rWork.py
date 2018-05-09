#!/user/bin/env python
'''rWork.py:

This filter returns True if the r_work value for this structure is within the
specified range

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"


class RWork(object):
    '''This filter returns True if the rWork value for this structure is within
    the specified range.

    Attributes
    ----------
    min_Rwork : float
       The lower bound r_work value
    max_Rwork : float
       The upper bound r_work value
    '''

    def __init__(self, minRwork, maxRwork):
        self.min_Rwork = minRwork
        self.max_Rwork = maxRwork

    def __call__(self, t):
        if t[1].r_work == None:
            return False

        return t[1].r_work >= self.min_Rwork and t[1].r_work <= self.max_Rwork
