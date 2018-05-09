#!/user/bin/env python
'''containsDProteinChain.py

This filter returns entries that contain protein chain(s) made of D-amino acids.
The default constructor returns entries that contain at least one
polymer chain that is an D-protein. If the "exclusive" flag is set to true
in the constructor, all polymer chains must be D-proteins. For a multi-model structure,
this filter only checks the first model.

'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.filters import ContainsPolymerChainType


class ContainsDProteinChain(object):
    '''Default constructor matches any entry that contains at least one D-protein chain.
    As an example a D-protein/DNA complex passes this filter

    Optional constructor that can be used to filter entries that exclusively
    contain D-protein chains. For example, with "exclusive" set to true, a
    D-protein/DNA complex does not pass this filter.

    Attributes
    ----------
    exclusive : bool
       if true, only return entries that are exclusively contain D-protein chains
    '''

    def __init__(self, exclusive=False):
        self.filter = ContainsPolymerChainType([
            ContainsPolymerChainType.D_PEPTIDE_LINKING,
            ContainsPolymerChainType.PEPTIDE_LINKING], exclusive=exclusive)

    def __call__(self, t):
        return self.filter(t)
