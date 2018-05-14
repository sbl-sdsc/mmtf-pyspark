#!/user/bin/env python
'''structureToAllInteractions.py:

Finds interactions of a specified group within a specified cutoff distance
'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from pyspark.sql import Row


class StructureToAllInteractions(object):
    '''Class that finds interactions of a specified group within a specified
    cutoff distance

    Attributes
    ----------
    groupName : str
       specified group in structure
    cutoffDistance : float
       cutoff distance used during search
    '''

    def __init__(self, groupName, cutoffDistance):
        self.groupName = groupName
        self.cutoffDistance = cutoffDistance

    def __call__(self, t):
        structureId = t[0]
        structure = t[1]

        groupIndices, groupNames = self._get_group_indices(structure)
        interactions = []

        for i in range(len(groupNames)):

            if groupNames[i] == self.groupName:
                matches = []
                boundingBox = self._calc_bonding_box(structure, groupIndices, i,
                                                     self.cutoffDistance)
                matches += self._find_neighbors(structure,
                                                i, boundingBox, groupIndices)
                interactions += self._get_distance_profile(
                    structureId, matches, i, groupIndices, groupNames, structure)

        return interactions

    def _get_distance_profile(self, structureId, matches, index, groupIndices, groupNames, structure):
        cutoffDistanceSq = self.cutoffDistance * self.cutoffDistance

        x = structure.x_coord_list
        y = structure.y_coord_list
        z = structure.z_coord_list

        first = groupIndices[index]
        last = groupIndices[index + 1]

        groupIndex1 = structure.group_type_list[index]

        rows = []

        for i in matches:
            if i == index:
                continue

            for j in range(groupIndices[i], groupIndices[i + 1]):

                for k in range(first, last):
                    dx = x[j] - x[k]
                    dy = y[j] - y[k]
                    dz = z[j] - z[k]
                    dSq = dx * dx + dy * dy + dz * dz

                    if (dSq <= cutoffDistanceSq):
                        aIndex1 = k - first
                        atomName1 = structure.group_list[groupIndex1]['atomNameList'][aIndex1]
                        element1 = structure.group_list[groupIndex1]['elementList'][aIndex1]

                        groupIndex2 = structure.group_type_list[i]
                        aIndex2 = j - groupIndices[i]
                        atomName2 = structure.group_list[groupIndex2]['atomNameList'][aIndex2]
                        element2 = structure.group_list[groupIndex2]['elementList'][aIndex2]

                        d = dSq ** 0.5
                        row = Row(structureId,
                                  groupNames[index],
                                  atomName1,
                                  element1,
                                  index,
                                  groupNames[i],
                                  atomName2,
                                  element2,
                                  i,
                                  float(d))
                        rows.append(row)
        return rows

    def _find_neighbors(self, structure, index, boundingBox, groupIndices):
        x = structure.x_coord_list
        y = structure.y_coord_list
        z = structure.z_coord_list

        matches = []
        for i in range(len(groupIndices) - 1):

            for j in range(groupIndices[i], groupIndices[i + 1]):

                if (x[j] >= boundingBox[0] and x[j] <= boundingBox[1] and
                    y[j] >= boundingBox[2] and y[j] <= boundingBox[3] and
                        z[j] >= boundingBox[4] and z[j] <= boundingBox[5]):
                    matches.append(i)
                    break

        return matches

    def _calc_bonding_box(self, structure, groupIndices, i, cutoffDistance):
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
        last = groupIndices[i + 1]

        for i in range(first, last):
            xMin = min(xMin, x[i])
            xMax = max(xMax, x[i])
            yMin = min(yMin, y[i])
            yMax = max(yMax, y[i])
            zMin = min(zMin, z[i])
            zMax = max(zMax, z[i])

        boundingBox = [0] * 6
        boundingBox[0] = float(xMin - cutoffDistance)
        boundingBox[1] = float(xMax + cutoffDistance)
        boundingBox[2] = float(yMin - cutoffDistance)
        boundingBox[3] = float(yMax + cutoffDistance)
        boundingBox[4] = float(zMin - cutoffDistance)
        boundingBox[5] = float(zMax + cutoffDistance)

        return boundingBox

    def _get_group_indices(self, structure):
        '''Creates an atom index to the first atom of each group

        Parameters
        ----------
        structure : mmtfStructure
        '''
        groupIndices, groupNames = [0], []  # Start index for first group
        atomCounter, groupCounter = 0, 0

        # Consider only the first model
        numChains = structure.chains_per_model[0]

        # Loop over all chains
        for i in range(numChains):

            # Loop over all groups in chains
            for j in range(structure.groups_per_chain[i]):
                groupIndex = structure.group_type_list[groupCounter]
                groupNames.append(
                    structure.group_list[groupIndex]['groupName'])
                atomCounter += len(
                    structure.group_list[groupIndex]['atomNameList'])
                groupIndices.append(atomCounter)
                groupCounter += 1

        return groupIndices, groupNames
