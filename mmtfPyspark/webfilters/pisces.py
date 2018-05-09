#!/user/bin/env python
'''pisces.py

This filter passes through representative structures or protein chains
from the PISCES CulledPDB sets. A CulledPDB set is selected by specifying
sequenceIdentity and resolution cutoff values from the following
list:
- sequenceIdentity = [20, 25, 30, 40, 50, 60, 70, 80, 90]
- resolution = [1.6, 1.8, 2.0, 2.2, 2.5, 3.0]

References
----------
- `PISCES <http://dunbrack.fccc.edu/PISCES.php>`_
- G. Wang and R. L. Dunbrack, Jr. PISCES: a protein sequence culling server.  Bioinformatics, 19:1589-1591, 2003.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.webservices import PiscesDownloader


class Pisces(object):
    '''Filters representative PDB structures and polymer chains based
    on the specified criteria using PISCES CulledPDB sets.

    sequenceIdentity = 20, 25, 30, 40, 50, 60, 70, 80, 90
    resolution = 1.6, 1.8, 2.0, 2.2, 2.5, 3.0

    Attributes
    ----------
    sequenceIdentity : int
       sequence identity cutoff values
    resolution : float
       resolution cutoff value
    '''

    def __init__(self, sequenceIdentity, resolution):
        self.pdbIds = set()

        pD = PiscesDownloader(sequenceIdentity, resolution)

        for pdbId in pD.get_structure_chain_ids():
            self.pdbIds.add(pdbId)
            self.pdbIds.add(pdbId[:4])

    def __call__(self, t):
        return t[0] in self.pdbIds
