#!/user/bin/env python
'''
depositionDate.py

this filter return turn if the deposition date for this
structure is within the specified range

authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from dateutil.parser import parse

class depositionDate(object):

    def __init__(self, startdate, enddate):
        self.startdate = parse(startdate)
        self.enddate = parse(enddate)


    def __call__(self,t):
        structure = t[1]
        depositiondate = parse(structure.deposition_date)

        return depositiondate >= self.startdate and depositiondate <= self.enddate
