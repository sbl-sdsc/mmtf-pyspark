#!/usr/bin/env python
#!/usr/bin/env python
'''
Simple example of reading an MMTF Hadoop Sequence file, filtering the entries \
by resolution,and counting the number of entries.

Authorship information:
__author__ = "Peter Rose"
__maintainer__ = "Mars Huang"
__email__ = "marshuang80@gmai.com:
__status__ = "Warning"
'''
# TODO Traceback "ResourceWarning: unclosed filecodeDecodeError: 'ascii' codec can't decode byte 0xc3 in position 25: ordinal not in range(128)"
# TODO No actual value for unit test

import unittest
from pyspark import SparkConf, SparkContext
from ..main.MmtfReader import readSequenceFile
from ..main.filters import resolution

path = '../full'
# TODO Change actual size
actual_size = 1000

class testResolutionFilter(unittest.TestCase):

    def test_resolution_return(self):
        self.assertEqual(actual_size,pdb.count())

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName('Demo0b')
    sc = SparkContext(conf=conf)
    pdb = readSequenceFile(path, sc)
    pdb = pdb.filter(resolution(0.0,2.0))
    unittest.main()
    sc.close()
