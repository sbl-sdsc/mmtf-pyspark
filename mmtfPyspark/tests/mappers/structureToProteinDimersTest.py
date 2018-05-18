#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.mappers import StructureToBioassembly, StructureToProteinDimers
from mmtfPyspark.io.mmtfReader import download_mmtf_files


class StructureToProteinDimersTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("structureToProteinDimers") \
                                 .getOrCreate()
        
    def test1(self):
        pdbIds = ["1I1G"]
        self.pdb = download_mmtf_files(pdbIds)

        pdb_1 = self.pdb.flatMap(StructureToBioassembly()) \
                        .flatMap(StructureToProteinDimers(8, 20, False, True))

        self.assertTrue(pdb_1.count() == 4)

    def test2(self):
        pdbIds = ["5NV3"]
        self.pdb = download_mmtf_files(pdbIds)

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
        self.pdb = download_mmtf_files(pdbIds)

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
        self.pdb = download_mmtf_files(pdbIds)

        pdb_4 = self.pdb.flatMap(StructureToBioassembly()) \
                        .flatMap(StructureToProteinDimers(9, 20, False, True))

        self.assertTrue(pdb_4.count() == 5)

    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
