#!/user/bin/env python
'''
releaseDate.py

this filter return turn if the releaseDate date for this
structure is within the specified range

authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "done"
'''

from dateutil.parser import parse

class releaseDate(object):

    def __init__(self, startDate, endDate):
        self.startDate = parse(startDate)
        self.endDate = parse(endDate)


    def __call__(self,t):
        structure = t[1]
        releaseDate = parse(structure.release_date)

        return releaseDate >= self.startDate and releaseDate <= self.endDate
