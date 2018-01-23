#!/user/bin/env python
'''
distanceBox.py:

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
'''
import numpy as np
from collections import defaultdict

class distanceBox(object):

    offset = [
        0 + ( 0 * 10000) + ( 0 * 1000000000),
	   -1 + (-1 * 10000) + (-1 * 1000000000),
	   -1 + (-1 * 10000) + ( 0 * 1000000000),
	   -1 + (-1 * 10000) + ( 1 * 1000000000),
	   -1 + ( 0 * 10000) + (-1 * 1000000000),
	   -1 + ( 0 * 10000) + ( 0 * 1000000000),
	   -1 + ( 0 * 10000) + ( 1 * 1000000000),
	   -1 + ( 1 * 10000) + (-1 * 1000000000),
	   -1 + ( 1 * 10000) + ( 0 * 1000000000),
	   -1 + ( 1 * 10000) + ( 1 * 1000000000),
		0 + (-1 * 10000) + (-1 * 1000000000),
		0 + (-1 * 10000) + ( 0 * 1000000000),
		0 + (-1 * 10000) + ( 1 * 1000000000),
		0 + ( 0 * 10000) + (-1 * 1000000000),
		0 + ( 0 * 10000) + ( 1 * 1000000000),
		0 + ( 1 * 10000) + (-1 * 1000000000),
		0 + ( 1 * 10000) + ( 0 * 1000000000),
		0 + ( 1 * 10000) + ( 1 * 1000000000),
		1 + (-1 * 10000) + (-1 * 1000000000),
		1 + (-1 * 10000) + ( 0 * 1000000000),
		1 + (-1 * 10000) + ( 1 * 1000000000),
		1 + ( 0 * 10000) + (-1 * 1000000000),
		1 + ( 0 * 10000) + ( 0 * 1000000000),
		1 + ( 0 * 10000) + ( 1 * 1000000000),
		1 + ( 1 * 10000) + (-1 * 1000000000),
		1 + ( 1 * 10000) + ( 0 * 1000000000),
		1 + ( 1 * 10000) + ( 1 * 1000000000)
        ]

    def __init__(self, binWidth):

        self.inverseBinWidth = 1.0/binWidth
        self.hashMap = defaultdict(list)


    def addPoint(self, point, pointName):

        i = np.rint(float(point.x) * self.inverseBinWidth)
        j = np.rint(float(point.y) * self.inverseBinWidth)
        k = np.rint(float(point.z) * self.inverseBinWidth)
        location = i + (j*10000) + (k*1000000000)

        #print(location, pointName)

        #if location in self.hashMap:
        #    print(location)
        #    print(self.hashMap[location])
        #self.hashMap[location] = self.hashMap[location].append(pointName)
        self.hashMap[location].append(pointName)

        #else:
        #    self.hashMap[location] = [pointName]



    def getIntersection(self, distanceBox):

        intersection = []
        checkedLocations = set()

        for location in self.hashMap.keys():

            overlap = False

            for off in self.offset:

                loc = location + off

                if loc in distanceBox.hashMap:
                    overlap = True

                    break

            if overlap:

                for off in self.offset:

                    loc = location + off

                    if loc in checkedLocations:
                        continue

                    checkedLocations.add(loc)
                    if loc in self.hashMap:
                        intersection += self.hashMap[loc]

        return intersection
