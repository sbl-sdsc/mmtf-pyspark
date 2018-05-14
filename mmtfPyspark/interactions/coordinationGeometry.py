#!/user/bin/env python
'''coordinateGeometry.py

This class calculates distances, angles, and orientational order parameters
for atoms coordinated to a central atom

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

import numpy as np
import itertools as it
import math


class CoordinateGeometry(object):
    '''A coordinate geoetry class that calcualtes distances angles and
    orientational order parameters.

    Attributes
    ----------
    center : list
       coordinates of the center atom
    neighbors : list
       coordinates of neighbor atoms
    '''

    def __init__(self, center, neighbors):

        self.center = center
        self.neighbors = neighbors
        self._calc_distances()
        self._calc_angles()
        self._calc_dot_products()

    def get_distance(self):
        '''Returns an array of distances from the coordination center to the
        neighbor atoms.

        Returns
        -------
        :obj:`list <numpy.ndarray>`
            Numpy array of pairwise angles
        '''

        return self.distances

    def get_angles(self):
        '''Returns all pairwise angles between a center and pairs of neighbor atoms.

        Returns
        -------
        :obj:`list <numpy.ndarray>`
            Numpy array of pairwise angles
        '''

        return self.angles

    def q3(self):
        '''Returns a normalized trigonal orientational order parameter q3. The
        orientational order parameter q3 measures the extent to which a molecule
        and its three nearest neighbors adopt a trigonal arrangement. It is equal
        to 0 for a random arrangement and equals 1 in a perfectly trigonal
        arrangement.

        References
        ----------
        - Richard H. Henchman and Stuart J. Cockramb (2013), Water’s Non-Tetrahedral 
          Side, Faraday Discuss., 167, 529.  https://dx.doi.org/10.1039/c3fd00080j

        Returns
        -------
        float
           trigonal orientational order parameter
        '''

        if len(self.neighbors) < 3:
            raise ValueError(f"ERROR: Trigonality calculation requires at least \
                             3 neighbors, but found: {len(self.neighbors)}")

        total = 0.0
        for i, j in it.combinations(range(3), 2):
            total += (self.dotProducts[i][j] + 0.5) ** 2

        return float(1.0 - 4.0 / 7.0 * total)

    def q4(self):
        '''Returns a normalized tetrahedral orientational order parameter q4. The
        orientational order parameter q4 measures the extent to which a central
        atom and its four nearest neighbors adopt a tetrahedral arrangement. It
        is equal to 0 for a random arrangement and equals 1 in a perfectly
        tetrahedral arrangement. It can reach a minimum value of -3 for unusual
        arrangements.

        References
        ----------
        Jeffrey R. Errington & Pablo G. Debenedetti (2001) Relationship between structural order and the anomalies of liquid water, Nature 409, 318-321.  <a href="https://dx.doi.org/10.1038/35053024">doi:10.1038/35053024</a>

        P.-L. Chau & A. J. Hardwick (1998) A new order parameter for tetrahedral configurations, Molecular Physics, 93:3, 511-518.  <a href"https://dx.doi.org/10.1080/002689798169195">doi:10.1080/002689798169195</a>

        Returns
        -------
        float
           tetrahedra orientational order parameter
        '''

        if len(self.neighbors) < 4:
            raise ValueError(f"ERROR: Tetrahedral calculation requires at least \
                             4 neighbors, but found: {len(self.neighbors)}")

        total = 0.0
        for i, j in it.combinations(range(4), 2):
            total += (self.dotProducts[i][j] + 1.0 / 3.0) ** 2

        return float(1.0 - 3.0 / 8.0 * total)

    def q5(self):
        '''Returns a normalized trigonal bipyramidal orientational order parameter
        q5. The first three nearest atoms to the center atom are used to define
        the equatorial positions. The next two nearest atoms are used to specify
        the axial positions. The orientational order parameter q5 measures the
        extent to which the five nearest atoms adopt a trigonal bipyramidal
        arrangement. It is equal to 0 for a random arrangement and equals 1 in a
        perfectly trigonal bipyramidal arrangement. It can reach negative values
        for certain arrangements.

        References
        ----------
        - `Richard H. Henchman and Stuart J. Cockramb (2013), Water’s Non-Tetrahedral Side, Faraday Discuss., 167, 529.
          <https://dx.doi.org/10.1039/c3fd00080j>`_
        
        Note, the summations in equation (3) in this paper is incorrect.
        This method uses the corrected version (R. Henchman, personal communication).

        Returns
        -------
        float
           trigonal bipyramidal orientational order parameter
        '''

        if len(self.neighbors) < 5:
            raise ValueError(f"ERROR: Tetrahedral calculation requires at least \
                             5 neighbors, but found: {len(self.neighbors)}")

        # 3 equatorial-equatorial angles (120 deg: cos(120) = -1/2)
        ee = [(0, 1), (0, 2), (1, 2)]
        sum1 = sum([(self.dotProducts[i][j] + 1.0 / 2.0) ** 2 for i, j in ee])

        # 6 equatorial-axial angles (90 deg: cos(90) = 0)
        ea = [(0, 3), (0, 4), (1, 3), (1, 4), (2, 3), (2, 4)]
        sum2 = sum([self.dotProducts[i][j] ** 2 for i, j in ea])

        # 1 axial-axial angle (180 deg: cos(180) = -1)
        sum3 = (self.dotProducts[3][4] + 1) ** 2

        return float(1.0 - 6.0 / 35.0 * sum1 - 3.0 / 10.0 * sum2 - 3.0 / 40.0 * sum3)

    def q6(self):
        '''Returns a normalized octahedra orientational order parameter q6.
        The orientational order parameter q6 measures the extent to which a
        central atom and its six nearest neighbors adopt an octahedral arrangement.
        It is equal to 0 for a random arrangement and equals 1 in a perfectly
        octahedralhedral arrangement. It can reach negative values for certain
        arrangements.

        References
        ----------
        - `Richard H. Henchman and Stuart J. Cockramb (2013), Water’s Non-Tetrahedral Side, Faraday Discuss., 167, 529.  <https://dx.doi.org/10.1039/c3fd00080j>`_

        The same method as described in this paper was used to derive the q6 parameter (R. Henchman, personal communication).

        Returns
        -------
        float
           Octahedral orientational order parameter
        '''

        if len(self.neighbors) < 6:
            raise ValueError(f"ERROR: Octrahedrality calculation requires at \
                             least 5 neighbors, but found: {len(self.neighbors)}")

        # 4 angles between positions in equatorial plane
        equatorial = [(0, 1), (1, 2), (2, 3), (3, 0)]
        sum1 = sum([self.dotProducts[i][j]**2 for i, j in equatorial])

        # 4 angles between axial position 1 and equatorial plane
        axial_1 = [(0, 4), (1, 4), (2, 4), (3, 4)]
        sum2 = sum([self.dotProducts[i][j]**2 for i, j in axial_1])

        # 4 angles between axial position 2 and equatorial plane
        axial_2 = [(0, 5), (1, 5), (2, 5), (3, 5)]
        sum3 = sum([self.dotProducts[i][j]**2 for i, j in axial_2])

        return float(1.0 - 1.0 / 4.0 * sum([sum1, sum2, sum3]))

    def _calc_distances(self):
        '''Calcualte and set the distances between the center and it's neighbors
        '''

        dist = [float(np.linalg.norm(self.center - n)) for n in self.neighbors]
        self.distances = dist

    def _calc_angles(self):
        '''Calcualte and set the angles between the center and it's neighbors
        '''

        self._get_vectors()
        self.angles = [0.0] * \
            int((len(self.neighbors) * len(self.neighbors) - 1) / 2)

        n = 0
        for i in range(len(self.vectors) - 1):
            for j in range(i + 1, len(self.vectors)):
                self.angles[n] = self._angle(self.vectors[i], self.vectors[j])
                n += 1

    def _calc_dot_products(self):
        '''Calcualte and set the dot products between the center and it's neighbors
        '''

        index = self._get_index_by_distance(self.distances)
        vectors = np.array([(self.center - self.neighbors[index[i]]) /
                            np.linalg.norm(
                                self.center - self.neighbors[index[i]])
                            for i in range(len(self.neighbors))])

        self.dotProducts = np.zeros((len(vectors), len(vectors)))
        for i in range(len(vectors) - 1):
            for j in range(i + 1, len(vectors)):
                self.dotProducts[i, j] = float(np.dot(vectors[i], vectors[j]))

    def _get_vectors(self):
        '''Set the center to neighbors vectors
        '''

        self.vectors = [self.center - n for n in self.neighbors]

    def _angle(self, a, b):
        '''Calculate the angle between two points

        Parameters 
        ----------
        a : list
           point a
        b : list
           oint b
        '''
        arccosInput = np.dot(a, b) / np.linalg.norm(a) / np.linalg.norm(b)
        arccosInput = 1.0 if arccosInput > 1.0 else arccosInput
        arccosInput = -1.0 if arccosInput < -1.0 else arccosInput

        return float(math.acos(arccosInput))

    def _get_index_by_distance(self, values):
        '''Get a list of indices sorted by their calculated distance
        '''

        indexed_values = [(index, value) for index, value in enumerate(values)]
        indexed_values.sort(key=lambda x: x[1])
        return [tup[0] for tup in indexed_values]
