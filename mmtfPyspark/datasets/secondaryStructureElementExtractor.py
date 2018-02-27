#!/user/bin/env python
'''secondaryStructureElementExtractor.py:

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from mmtfPyspark.ml import pythonRDDToDataset
from mmtfPyspark.mappers import StructureToSecondaryStructureElements
from mmtfPyspark.datasets import secondaryStructureExtractor

def getDataset(structure, label, length=None):
    '''
    # TODO comment
    '''

    colNames = ["sequence", "label"]

    if length == None:

        rows = secondaryStructureExtractor.getPythonRdd(structure) \
               .flatMap(StructureToSecondaryStructureElements(label))

        return pythonRDDToDataset.getDataset(rows, colNames)
    else :

        rows = secondaryStructureExtractor.getPythonRdd(structure) \
               .flatMap(StructureToSecondaryStructureElements(label, length))

        return pythonRDDToDataset.getDataset(rows, colNames)
