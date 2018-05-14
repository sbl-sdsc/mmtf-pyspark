#!/user/bin/env python
'''secondaryStructureElementExtractor.py

Returns a datset of continuous segments of protein sequence with the specified
DSSP secondary structure code (E, H, C) of a minimum length.

Examples
--------
+-------------+-----+
|sequence     |label|
+-------------+-----+
|TFIVTA       |E    |
|ALTGTYE      |E    |
+-------------+-----+

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.ml import pythonRDDToDataset
from mmtfPyspark.mappers import StructureToSecondaryStructureElements
from mmtfPyspark.datasets import secondaryStructureExtractor


def get_dataset(structure, label, length=None):
    '''Returns a dataset of continuous segments of protein sequence with the
    specified DSSP secondary structure code (E, H, C) of a minimum length.

    Parameters
    ----------
    structure : structure
    label : str 
       DSSP secondary structure label (E, H, C)
    length : int
       minimum length of secondary structure segment

    Returns
    -------
    dataset
        dataset of continuous segments of protein sequence
    '''

    colNames = ["sequence", "label"]

    if length == None:

        rows = secondaryStructureExtractor.get_python_rdd(structure) \
            .flatMap(StructureToSecondaryStructureElements(label))

        return pythonRDDToDataset.get_dataset(rows, colNames)
    else:

        rows = secondaryStructureExtractor.get_python_rdd(structure) \
            .flatMap(StructureToSecondaryStructureElements(label, length))

        return pythonRDDToDataset.get_dataset(rows, colNames)
