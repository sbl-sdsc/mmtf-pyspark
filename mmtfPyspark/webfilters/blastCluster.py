#!/user/bin/env python
'''blastCluster.py

This filter passes through representative structures from the RCSB PDB
BlastCLust cluster. A sequence identity thresholds needs to be specified.
The representative for each cluster is the first chain in a cluster.

References
----------
BlastClust cluster field names:
`Field names <http://www.rcsb.org/pdb/statistics/clusterStatistics.do>`_

Examples
--------
Find representative PDB entries at 90% sequence identity:
>>> sequenceIdentity = 90
>>> pdb = pdb.filter(BlastCluster(90))

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import urllib.request


class BlastCluster(object):
    '''Filters blast clusters

    Attributes
    ----------
    sequenceIdentity : int
       sequence indentity for blast
    '''
    def __init__(self, sequenceIdentity):

        clusters = self.get_blast_cluster(sequenceIdentity)

        self.pdbIds = set()

        for protein in clusters:
            self.pdbIds.add(protein)
            self.pdbIds.add(protein[:4])


    def __call__(self, t):
        return t[0] in self.pdbIds


    def get_blast_cluster(self, sequenceIdentity):

        if sequenceIdentity not in [30,40,50,70,90,95,100]:
            raise Exception(f"Error: representative chains are not availible for \
                            sequence Identity {sequenceIdentity}.\n Must be in \
                            range [30,40,50,70,90,95,100]")
            return

        coreUrl = "ftp://resources.rcsb.org/sequence/clusters/"
        clusters = []
        inputStream = urllib.request.urlopen(f"{coreUrl}bc-{sequenceIdentity}.out")

        for line in inputStream:
            line = str(line)[2:-3].replace("_",".").strip("\\n")
            clusters += line.split(" ")

        return clusters
