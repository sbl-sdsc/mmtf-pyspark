#!/user/bin/env python
'''
polymerSequenceExtractor.py:

Creates a dataset of polymer sequences using the full sequence
used in the experiment (i.e., the "SEQRES" record in PDB files).

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Debug"
'''

from src.main.ml import pythonRDDToDataset
from src.main.mappers import strctureToPolymerSquences
from pyspark.sql import Row


def getDataset(structures):
    '''
    Returns a dataset of polymer sequence contained in PDB entries
    using the full sequence used in the experimnet
    (i.e., the "SEQRES" record in PDB files)

    Attributes:
        structures (pythonRDD): a set of PDB structures
    Returns:
        dataset with interacting residue and atom information
    '''

    rows = structures.flatMap(structureToPolymerSquences()) \
                     .map(lambda x: Row(x[0],x[1]))

    colNames = ["structureChainId", "sequence"]
    return pythonRDDToDataset.getDataset(rows, colNames)
