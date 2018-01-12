#!/usr/bin/env python
'''
keywordSearch.py

PDBj Mine 2 RDB keyword search query and MMTF filtering using pdbid
This filter searches the 'keyword' column in the brief_summary table for
a keyword and returns a couple of columns for the matching entries

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from pyspark import SparkConf, SparkContext
from mmtfPyspark.webfilters import PdbjMine
from mmtfPyspark.datasets import PdbjMineService
from mmtfPyspark.io import MmtfReader

def main():
    path = "/home/marshuang80/PDB/full"

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("keywordSearch")
    sc = SparkContext(conf = conf)

    pdb = MmtfReader.readSequenceFile(path, sc)
    sql = "select pdbid, resolution, biol_species, db_uniprot, db_pfam, hit_score from keyword_search('porin') order by hit_score desc"

    search = PdbjMine(sql)
    count = pdb.filter(search).keys().count()
    print(f"Number of entries using sql to filter: {count}")

    dataset = PdbjMineService.getDataset(sql)
    dataset.show(10)
    search = PdbjMine(dataset = dataset)
    count = pdb.filter(search).keys().count()
    print(f"Number of entries using dataset to filter: {count}")

    sc.stop()

if __name__ == "__main__":
    main()
