#!/usr/bin/env python

import unittest
from pyspark.sql import SparkSession
from mmtfPyspark.io.mmtfReader import download_mmtf_files, read_sequence_file
from mmtfPyspark.interactions import InteractionFilter
from mmtfPyspark.interactions.interaction_extractor import InteractionExtractor
import time





class LigandInteractionFingerprintTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
                                 .appName("LigandInteractionFingerprintTest") \
                                 .getOrCreate()

        t0 = time.time()
        #raw = read_raw_sequence_file("/Users/peter/MMTF_Files/full")  # 85 sec
        #gp = raw.mapValues(lambda t: default_api.ungzip_data(t))  # 91
        # gp = raw.mapValues(lambda t: default_api.ungzip_data(t).read())  # 133 sec
        #gp = raw.map(lambda t: (t[0], default_api.ungzip_data(t[1]).read()))  # 131 sec
        #gp = raw.map(lambda t: (t[0], msgpack.unpackb(default_api.ungzip_data(t[1]).read(), raw=False)))  # 553 sec
        #gp = raw.map(lambda t: (t[0], pd.read_msgpack(gzip.decompress(t[1]))))  # 169 sec [4]:144
        ## convert directly to columnar structure?, lazy decoding?

        #gc.disable()  # 643 with gc disabled
        #gp = raw.map(lambda t: (t[0], MmtfStructure(msgpack.unpackb(unzip_data(t[1]), raw=False))))  ## 664 sec
        #gp = raw.map(lambda t: (t[0], MmtfStructure(msgpack.unpackb(default_api.ungzip_data(t[1]).read(), raw=False))))  ## 664 sec
        # gp = raw.mapValues(lambda t: MmtfStructure(msgpack.unpackb(default_api.ungzip_data(t).read(), raw=False)))  # 653 sec
        #func1 = default_api.ungzip_data  # try local version
        #func2 = msgpack.unpackb
        #func3 = MmtfStructure
        #gp = raw.map(lambda t: (t[0], MmtfStructure(func2(func1(t[1]).read(), raw=False))))  # 640 sec
        #gp = raw.mapValues(lambda t: func3(func2(func1(t).read(), raw=False)))  # 615

        #print("partitions:", gp.getNumPartitions())
        #print(gp.count())
        #t1 = time.time()
        #print("raw:", t1-t0)
        self.pdb = read_sequence_file("../../../resources/mmtf_full_sample")
        #print("partitions:", self.pdb.getNumPartitions())
        #self.pdb = read_sequence_file("/Users/peter/MMTF_Files/full")
        #self.pdb = self.pdb.filter(lambda s: s[1].num_models == 1)
        #print(self.pdb.count())
        #t1 = time.time()
        #print("raw:", t1-t0)

        #self.pdb = download_mmtf_files(['1OHR', '1STP', '1HWK'])
        #self.pdb = download_mmtf_files(['1STP'])
        #print(self.pdb.count())
        #t0 = time.time()
        #numAtoms = self.pdb.map(lambda t: t[1].num_atoms).reduce(lambda a, b: a + b)
        #print(f"Test-PDB: Total number of atoms in PDB: {numAtoms}")
        #t1 = time.time()
        #print("pdb: ", t1 - t0)



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
        #interactionFilter.set_distance_cutoff(4.0)
        interactionFilter.set_distance_cutoff(3.0)
        #interactionFilter.set_target_elements(True, ['O'])
        interactionFilter.set_target_elements(False, ['C','H','P'])
        metals = {"V", "CR", "MN", "MN3", "FE", "FE2", "CO", "3CO", "NI", "3NI", "CU", "CU1", "CU3", "ZN", "MO", "4MO",
                 "6MO"}
        interactionFilter.set_query_groups(True, metals)
        #interactionFilter.set_query_elements(False, ['C', 'H', 'P'])
        #interactionFilter.set_query_groups(False, "HOH")  # ignore water interactions

        t0 = time.time()
        interactions = InteractionExtractor.get_ligand_polymer_interactions(self.pdb, interactionFilter, level='group')
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

