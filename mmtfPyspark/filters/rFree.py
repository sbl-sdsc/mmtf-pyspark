#!/user/bin/env python
'''rFree.py:

This filter returns true if the rFree value for this structure is within the
specified range

References
----------
- `rFree <http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/r-value-and-r-free>`_

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"


class RFree(object):
    '''This filter returns True if the rFree value for this structure in withing
    the specified range.

    Attributes
    ----------
    min_Rfree : float
       The lower bound r_free value
    max_RFree : float
       The upper bound r_free value
    '''

    def __init__(self, minRfree, maxRfree):
        self.min_Rfree = minRfree
        self.max_Rfree = maxRfree

    def __call__(self, t):
        if t[1].r_free == None:
            return False

        return (t[1].r_free >= self.min_Rfree and t[1].r_free <= self.max_Rfree)
