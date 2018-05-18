#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files
from mmtfPyspark.interactions import InteractionFilter
from mmtfPyspark.interactions import InteractionFingerprinter

class PolymerInteractionFingerprintTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("PolymerInteractionFingerprint") \
                                 .getOrCreate()
                                 
        # download structure 1OHR
        self.pdb = download_mmtf_files(['1OHR'])



    def test1(self):

        interactionFilter = InteractionFilter(distanceCutoff=4.0, minInteractions=10)
        interactions = InteractionFingerprinter.get_polymer_interactions(self.pdb, interactionFilter)
        self.assertTrue(interactions.count() == 2)

    def test2(self):

        interactionFilter = InteractionFilter(distanceCutoff=3.5, minInteractions=1)
        interactionFilter.set_query_groups(True, ['ASP'])
        interactionFilter.set_query_atom_names(True, ['OD1', 'OD2'])
        interactionFilter.set_target_groups(True, ['ARG'])
        interactionFilter.set_target_atom_names(True, ['NH1', 'NH2'])

        interactions = InteractionFingerprinter.get_polymer_interactions(self.pdb, interactionFilter)
        self.assertTrue(interactions.count() == 2)

    def tearDown(self):
        self.spark.stop()
        pass

if __name__ == '__main__':
    unittest.main()
