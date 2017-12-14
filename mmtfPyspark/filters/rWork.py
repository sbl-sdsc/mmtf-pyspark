#!/user/bin/env python
'''
rWork.py:

This filter return turn if the r_work value for this structure is within the
specified range

Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

class rWork(object):
    '''
    Attributes:
        min_Rwork (float): The lower bound r_work value
        max_Rwork (float): The upper bound r_work value
    '''
    def __init__(self, minRwork, maxRwork):
        self.min_Rwork = minRwork
        self.max_Rwork = maxRwork


    def __call__(self,t):
        if t[1].r_work == None:
            return False

        return t[1].r_work >= self.min_Rwork and t[1].r_work <= self.max_Rwork
