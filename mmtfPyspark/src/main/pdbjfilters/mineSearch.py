#!/user/bin/env python
'''
mineSearch.py

This filter runs an PDBj Mine 2 Search web service using SQL query

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

class mineSearch(object):
    '''
    Fetch data using the PDBj Mine 2 SQL service
    '''

    SERVICELOCATION="https://pdbj.org/rest/mine2_sql"

    def __init__(sqlQuery, pdbidField = "pdbid", chainLevel = False):

        self.sqlQuery = sqlQuery
        self.pdbidField = pdbidField
        self.chainLevel = chainLevel

        # TODO encodedSQL = URLEncoder.encoder
