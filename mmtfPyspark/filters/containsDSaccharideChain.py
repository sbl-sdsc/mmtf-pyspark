#!/user/bin/env python
'''containsDSaccharideChain.py

This filter returns entries that contain chain(s) made of linear and branched D-saccharides.
The default constructor returns entries that contain at least one
polymer chain that is a D-saccharides. If the "exclusive" flag is set to true
in the constructor, all polymer chains must be D-saccharides. For a multi-model structure,
this filter only checks the first model.

.. note::

Since the PDB released PDBx/mmCIF version 5.0 in July 2017, it appears that
all polysaccharides have been converted to monomers. Therefore, this filter
does not return any results.

'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"


from mmtfPyspark.filters import ContainsPolymerChainType


class ContainsDSaccharideChain(object):
    '''Default constructor matches any entry that contains at least one chain
    made of D-saccharides. As an example, a glycosylated protein complex passes
    this filter.

    Optional constructor that can be used to filter entries that exclusively
    contain D-saccharide chains. For example, with "exclusive" set to true, an
    D-saccharide/protein does not pass this filter.

    Attributes
    ----------
    exclusive : bool
       if true, only return entries that are exclusively contain D-saccharide chains

    '''

    def __init__(self, exclusive=False):
        self.filter = ContainsPolymerChainType([
            ContainsPolymerChainType.D_SACCHARIDE,
            ContainsPolymerChainType.SACCHARIDE,
            ContainsPolymerChainType.D_SACCHARIDE_14_and_14_LINKING,
            ContainsPolymerChainType.D_SACCHARIDE_14_and_16_LINKING
        ], exclusive=exclusive)

    def __call__(self, t):
        return self.filter(t)
