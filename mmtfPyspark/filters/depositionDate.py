#!/user/bin/env python
'''depositionDate.py

This filter return true if the deposition date of this structure is within the
specified range

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from dateutil.parser import parse


class DepositionDate(object):

    def __init__(self, startdate, enddate):
        '''This filter return True if the deposition date of this structure is
        within the specified range

        Parameters
        ----------
        startdate : str
           start of the deposition date range
        enddate : str
           end of the deposition date range
        '''
        self.startdate = parse(startdate)
        self.enddate = parse(enddate)

    def __call__(self, t):
        structure = t[1]
        depositiondate = parse(structure.deposition_date)

        return depositiondate >= self.startdate and depositiondate <= self.enddate
