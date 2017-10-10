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
from src.main.pdbjfilters import mineSearch
from src.main.io import MmtfReader

def main():
    path = "/home/marshuang80/PDB/full"

    conf = SparkConf().setMaster("local[*]") \
                      .setAppName("keywordSearch")
    sc = SparkContext(conf = conf)

    pdb = MmtfReader.readSequenceFile(path, sc)
    sql = "select pdbid, resolution, biol_species, db_uniprot, db_pfam, hit_score from keyword_search('porin') order by hit_score desc"

    search = mineSearch(sql)

    #print(pdb.filter(mineSearch(sql)).keys().count())
    search.dataset.show(10)

    print("Number of entries in MMTF library matching query: " + \
    # TODO: unable to use filter when dataset is set to global
    #      str(pdb.filter(search).keys().count()) + "/" + \
          str(search.dataset.count()))


    sc.stop()

if __name__ == "__main__":
    main()
