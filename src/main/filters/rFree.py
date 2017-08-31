#!/user/bin/env python
'''
rFree.py:

This filter return turn if the rFree value for this structure is within the
specified range

See: <a href="http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/r-value-and-r-free">rfree</a>


Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''


class rFree(object):
    '''
        Attributes:
        min_Rfree (float): The lower bound r_free value
        max_Rfree (float): The upper bound r_free value
    '''

    def __init__(self, minRfree, maxRfree):
        self.min_Rfree = minRfree
        self.max_Rfree = maxRfree

    def __call__(self, t):
        if t[1].r_free == None:
            return False

        return (t[1].r_free >= self.min_Rfree and t[1].r_free <= self.max_Rfree)
