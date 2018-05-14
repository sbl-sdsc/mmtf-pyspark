#!/user/bin/env python
'''releaseDate.py

this filter returns true if the release date for this
structure is within the specified range

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from dateutil.parser import parse


class ReleaseDate(object):

    def __init__(self, startDate, endDate):
        '''This filter retuns true if the release date for the structure is
        within the specified range.

        Parameters
        ----------
        startDate : str
           start of the release date range
        enddate : str
           end of the the release date range
        '''
        self.startDate = parse(startDate)
        self.endDate = parse(endDate)

    def __call__(self, t):
        structure = t[1]
        releaseDate = parse(structure.release_date)

        return releaseDate >= self.startDate and releaseDate <= self.endDate
