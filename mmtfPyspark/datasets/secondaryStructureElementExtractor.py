#!/user/bin/env python
'''
secondaryStructureElementExtractor.py:

# TODO comment

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Debug"
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
        print(rows.collect())
        return pythonRDDToDataset.getDataset(rows, colNames)
