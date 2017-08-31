#!/user/bin/env python
'''
resolution.py:

This filter return true if the resolution value for this
structure is within the sepcified range

See: <a href="dhttp://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/resolution">resolution</a>

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

class resolution(object):
    '''
    Attributes:
        min_resolution (float): The lower bound resolution
        max_resolution (float): The upper bound resolution
    '''
    def __init__(self, minResolution, maxResolution):
        self.min_Resolution = minResolution
        self.max_Resolution = maxResolution


    def __call__(self,t):
        if t[1].resolution == None:
            return False
            
        return t[1].resolution >= self.min_Resolution and t[1].resolution <= self.max_Resolution
