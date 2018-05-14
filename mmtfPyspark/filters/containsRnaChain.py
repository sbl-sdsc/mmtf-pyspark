#!/user/bin/env python
'''containsRnaChain.py

This filter passes entries that contain RNA chains. The default constructor
passes entries that contain at least one RNA chain. If the "exclusive" flag is
set to true in the constructor, all polymer chains must be RNA. For a multi-model
structure (e.g., NMR structure), this filter only checks the first model.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"
from mmtfPyspark.filters import ContainsPolymerChainType


class ContainsRnaChain(object):
    '''Default constructor matches any entry that contains at least one RNA
    chain. As an example, an RNA-protein complex passes this filter.

    Optional constructor that can be used to filter entries that exclusively
    contain DNA chains. For example, with "exclusive" set to true, an
    RNA-protein complex complex does not pass this filter.

    Parameters
    ----------
    exclusive : bool
       if true, only return entries that contain RNA chains
    '''

    def __init__(self, exclusive=False):
        self.filter = ContainsPolymerChainType(
            ContainsPolymerChainType.RNA_LINKING, exclusive)

    def __call__(self, t):
        return self.filter(t)
