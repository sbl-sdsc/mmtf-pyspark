#!/user/bin/env python
'''
structureToInteractingResidues.py:
Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
'''
from pyspark.sql import Row

class structureToInteractingResidues(object):
    '''This mapper convert a full format of the file to a reduced format
    Attributes:
    '''
    def __init__(self, groupName, cutoffDistance):
        '''This class initializer ...
        Args:
        '''
        self.groupName = groupName
        self.cutoffDistance = cutoffDistance


    def __call__(self, t):
        structureId = t[0]
        structure = t[1]
        groupIndices, groupNames = self._getGroupIndices(structure)

        neighbors = []
        for i in range(len(groupName)):

            if groupNames[i] == self.groupName:
                matches = []
                boundingBox = self._calcBondingBox(structure, groupIndices, i, self.cutoffDistance)
                matches += self._findNeighbors(structure, index, boudningBox, groupIndices)
                neighbors += self._getDistanceProfile(structureId,
                                  matches, i, groupIndices, groupNames, structure)
        return neighbors


    def _getDistanceProfile(self, structureId, matches, index, groupIndices, groupNames, structure):
        cutoffDistanceSq = cutoffDistance * cutoffDistance

        x = structure.x_coord_list
        y = structure.y_coord_list
        z = structure.z_coord_list

        first = groupIndices(index)
        last = groupIndices(index + 1)

        rows = []

        for i in matches:
            if i == index: continue

            minDSq = float('inf')
            minIndex = -1

            for j in range(groupIndices[i], groupIndices[i+1]):

                for k in range(first, last):
                    dx = x[j] - x[k]
                    dy = y[j] - y[k]
                    dz = z[j] - z[k]
                    dSq = dx*dx + dy*dy + dz*dz

                    if (dSq <= cutoffDistanceSq and dSq < minDSq):
                        minDSq = min(minDSq, dSq)
                        minIndex = i

            if minIndex >= 0:
                # TODO add unique group (and atom?) for each group?
                row = Row(structureId,
                          groupNames[index],
                          index,
                          groupNames.get(minIndex),
                          minIndex,
                          float(minDSq**0.5))
                rows.append(row)

        return rows


    def _findNeighbors(self, structure, index, boundingBox, groupIndices):
        x = structure.x_coord_list
        y = structure.y_coord_list
        z = structure.z_coord_list

        matches = []
        for i in range(len(groupIndices) - 1):

            for j in range(groupIndices[i], groupIndices[i+1]):

                if (x[j] >= boundingBox[0] and x[j] <= boundingBox[1] and
                    y[j] >= boundingBox[2] and y[j] <= boundingBox[3] and
                    z[j] >= boundindBox[4] and z[j] <= boundingBox[5]):
                    matches.append(i)
                    break

        return matches


    def _calcBondingBox(self, structure, groupIndices, i, cutoffDistance):
        x = structure.x_coord_list
        y = structure.y_coord_list
        z = structure.z_coord_list

        xMin = -float('inf')
        xMax = float('inf')
        yMin = -float('inf')
        yMax = float('inf')
        zMin = -float('inf')
        zMax = float('inf')

        first = groupIndices[i]
        last = groupIndices[i+1]

        for i in range(first, last):
            xMin = min(xMin, x[i])
            xMax = max(xMax, x[i])
            yMin = min(yMin, y[i])
            yMax = max(yMax, y[i])
            zMin = min(zMin, z[i])
            zMax = max(zMax, z[i])

        boundingBox = [0]*6
        boundingBox[0] = float(xMin - cutoffDistance)
        boundingBox[1] = float(xMax + cutoffDistance)
        boundingBox[2] = float(yMin - cutoffDistance)
        boundingBox[3] = float(yMax + cutoffDistance)
        boundingBox[4] = float(zMin - cutoffDistance)
        boundingBox[5] = float(zMax + cutoffDistance)

        return boundingBox


    def _getGroupIndices(self, structure):
        groupIndices = [0]
        groupNames = []

        atomCounter = 0
        groupCounter = 0
        numChains = structure.chains_per_model[0]

        for i in range(numChains):

            for j in range(structure.groups_per_chain[i]):
                groupIndex = structure.group_type_list[groupCounter]
                groupNames.append(structure.group_list[groupIndex]['groupName'])
                atomCounter += len(structure.group_list[group_Index]['bondAtomList'])
                groupIndices.append(atomCounter)
                groupCounter += 1

        return groupIndices, groupNames
