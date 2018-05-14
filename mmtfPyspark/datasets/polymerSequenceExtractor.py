#!/user/bin/env python
'''polymerSequenceExtractor.py:

Creates a dataset of polymer sequences using the full sequence
used in the experiment (i.e., the "SEQRES" record in PDB files).

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Debug"

from mmtfPyspark.ml import pythonRDDToDataset
from mmtfPyspark.mappers import StructureToPolymerSequences
from pyspark.sql import Row


def get_dataset(structures):
    '''Returns a dataset of polymer sequence contained in PDB entries
    using the full sequence used in the experimnet
    (i.e., the "SEQRES" record in PDB files)

    Parameters
    ----------
    structures : pythonRDD
       a set of PDB structures

    Returns
    -------
    dataset
       dataset with interacting residue and atom information
    '''

    rows = structures.flatMap(StructureToPolymerSequences()) \
                     .map(lambda x: Row(x[0],x[1]))

    colNames = ["structureChainId", "sequence"]

    return pythonRDDToDataset.get_dataset(rows, colNames)
