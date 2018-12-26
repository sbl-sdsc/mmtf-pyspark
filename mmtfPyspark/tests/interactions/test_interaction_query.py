#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.filters import ExperimentalMethods
from mmtfPyspark.io.mmtfReader import download_mmtf_files, read_sequence_file
from mmtfPyspark.interactions import InteractionFilter, InteractionFingerprinter
from mmtfPyspark.utils import ColumnarStructure
import time


class LigandInteractionFingerprintTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("LigandInteractionFingerprintTest") \
                                 .getOrCreate()

        self.pdb = read_sequence_file("/Users/peter/GitRespositories/mmtf-pyspark/resources/mmtf_full_sample")
        #self.pdb = read_sequence_file("/Users/peter/MMTF_Files/full")
        self.pdb = self.pdb.filter(lambda s: s[1].num_models == 1)
        #self.pdb = download_mmtf_files(['1OHR', '1STP', '1HWK'])
        #self.pdb = download_mmtf_files(['1OHR'])

    # def testm1(self):
    #     t0 = time.time()
    #     numAtoms = self.pdb.map(lambda t: t[1].num_atoms).reduce(lambda a, b: a + b)
    #     print(f"Test-PDB: Total number of atoms in PDB: {numAtoms}")
    #     t1 = time.time()
    #     print("pdb: ", t1 - t0)
    #
    # def test0(self):
    #     t0 = time.time()
    #     col = self.pdb.map(lambda s: ColumnarStructure(s[1], True))
    #     numAtoms = col.map(lambda t: t.get_num_atoms()).reduce(lambda a, b: a + b)
    #     print(f"Test-Col: Total number of atoms in PDB: {numAtoms}")
    #     t1 = time.time()
    #     print("ColumnarStructure: ", t1 - t0)

    def test1(self):
        interactionFilter = InteractionFilter()
        interactionFilter.set_distance_cutoff(3.0)
        # interactionFilter.set_target_elements(True, ['O','N','S'])
        interactionFilter.set_target_elements(False, ['C','H','P'])
        metals = {"V", "CR", "MN", "MN3", "FE", "FE2", "CO", "3CO", "NI", "3NI", "CU", "CU1", "CU3", "ZN", "MO", "4MO",
                  "6MO"}
        interactionFilter.set_query_groups(True, metals)
        #interactionFilter.set_query_elements(True, ['O', 'N', 'S'])
        #interactionFilter.set_query_groups(False, "HOH")  # ignore water interactions

        t0 = time.time()
        interactions = InteractionFingerprinter.get_ligand_polymer_interactions(self.pdb, interactionFilter).cache()
        interactions.show()
        print(interactions.count())
        t1 = time.time()
        print("Interactions: ", t1-t0)

        # self.assertTrue('4QXX' in results_1)
        # self.assertFalse('2ONX' in results_1)





    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
