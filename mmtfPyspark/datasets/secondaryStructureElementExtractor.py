#!/user/bin/env python
'''
secondaryStructureElementExtractor.py:

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from mmtfPyspark.ml import pythonRDDToDataset
from mmtfPyspark.mappers import structureToSecondaryStructureElements
from mmtfPyspark.datasets import secondaryStructureExtractor

def getDataset(structure, label, length=None):
    '''
    # TODO comment
    '''

    colNames = ["sequence", "label"]

    if length == None:

        rows = secondaryStructureExtractor.getPythonRdd(structure) \
               .flatMap(structureToSecondaryStructureElements(label))

        return pythonRDDToDataset.getDataset(rows, colNames)
    else :

        rows = secondaryStructureExtractor.getPythonRdd(structure) \
               .flatMap(structureToSecondaryStructureElements(label, length))

        return pythonRDDToDataset.getDataset(rows, colNames)
