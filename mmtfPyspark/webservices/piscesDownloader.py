#!/user/bin/env python
'''piscesDownloader.py

This class downloads representative protein chains from the PISCES
CulledPDB sets. A CulledPDB set is selected by specifying
sequenceIdentity and resolution cutoff values from the following
list:

- sequenceIdentity = [20, 25, 30, 40, 50, 60, 70, 80, 90]
- resolution = [1.6, 1.8, 2.0, 2.2, 2.5, 3.0]

References
----------
- PISCES: http://dunbrack.fccc.edu/PISCES.php
- G. Wang and R. L. Dunbrack, Jr. PISCES: a protein sequence culling server. Bioinformatics, 19:1589-1591, 2003.
'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from urllib.request import urlopen
import gzip


class PiscesDownloader(object):
    '''Downloads representative protein chains from the PISCES
    CulledPDB sets. A CulledPDB set is selected by specifying
    sequenceIdentity and resolution cutoff values from the following lists:
    - sequenceIdentity = 20, 25, 30, 40, 50, 60, 70, 80, 90
    - resolution = 1.6, 1.8, 2.0, 2.2, 2.5, 3.0

    Attributes
    ----------
    sequenceIdentity : int
       sequence identity [0]
    resoltion : int
       resoltion for pisces filter [0.0]
    '''

    URL = "http://dunbrack.fccc.edu/Guoli/culledpdb_hh"
    SEQ_ID_LIST = [20, 25, 30, 40, 50, 60, 70, 80, 90]
    RESOLUTION_LIST = [1.6, 1.8, 2.0, 2.2, 2.5, 3.0]

    def __init__(self, sequenceIdentity=0, resolution=0.0):

        if sequenceIdentity not in self.SEQ_ID_LIST:
            raise ValueError("Invalid sequenceIdentity")

        if resolution not in self.RESOLUTION_LIST:
            raise ValueError("Invalid resolution value")

        self.sequenceIdentity = sequenceIdentity
        self.resolution = resolution

    def get_structure_chain_ids(self):
        fileURL = self.URL + '/' + self._get_file_name()
        u = urlopen(fileURL)
        line = str(gzip.GzipFile(fileobj=u).read()).split('\\n')
        structureChainId = [l.split()[0][:4] + '.' + l.split()[0][4] for l in line if
                            len(l.split()) > 1]
        return structureChainId

    def _get_file_name(self):
        u = urlopen(self.URL)
        fileName = ""
        cs = "pc" + str(self.sequenceIdentity) + "_res" + str(self.resolution)

        while True:

            line = str(u.readline())
            line = line.split('\"')[1].split('/')[-1]
            if line[:3] == 'log':
                break

            if (cs in line) and ("fasta" not in line):
                fileName = line
                break

        u.close()
        return fileName
