#!/usr/bin/env python

import unittest
from pyspark import SparkConf, SparkContext
from mmtfPyspark.mappers import StructureToBioassembly, StructureToProteinDimers
from mmtfPyspark.io.mmtfReader import download_mmtf_files


class StructureToProteinDimersTest(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setMaster(
            "local[*]").setAppName('structureToProteinDimers')
        self.sc = SparkContext(conf=conf)

    def test1(self):
        pdbIds = ["1I1G"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

        pdb_1 = self.pdb.flatMap(StructureToBioassembly()) \
                        .flatMap(StructureToProteinDimers(8, 20, False, True))

        self.assertTrue(pdb_1.count() == 4)

    def test2(self):
        pdbIds = ["5NV3"]
        self.pdb = download_mmtf_files(pdbIds, self.sc)

        pdb_2 = self.pdb.flatMap(StructureToBioassembly()) \
                        .flatMap(StructureToProteinDimers(8, 20, False, True))

        self.assertTrue(pdb_2.count() == 12)

    def test3(self):
        pdbIds = ["4GIS"]
        # A3-A2
        # A4-A1
        # B5-A1
        # B6-A2
        # B6-B5
        # B7-A3
        # B7-A4
        # B8-A4
        # B8-B7
        self.pdb = download_mmtf_files(pdbIds, self.sc)

        pdb_3 = self.pdb.flatMap(StructureToBioassembly()) \
                        .flatMap(StructureToProteinDimers(8, 20, False, True))

        self.assertTrue(pdb_3.count() == 9)

    def test4(self):
        pdbIds = ["1BZ5"]
        # C5-B4
        # C6-B3
        # D7-A2
        # D8-A1
        # E10-E9
        self.pdb = download_mmtf_files(pdbIds, self.sc)

        pdb_4 = self.pdb.flatMap(StructureToBioassembly()) \
                        .flatMap(StructureToProteinDimers(9, 20, False, True))

        self.assertTrue(pdb_3.count() == 5)

    def tearDown(self):
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
