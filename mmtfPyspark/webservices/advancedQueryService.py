#!/user/bin/env python
'''advancedQueryService.py

Post an XML query (PDB XML query format) to the RESTful RCSB web service

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import urllib

SERVICELOCATION = "http://www.rcsb.org/pdb/rest/search"
NEW_SERVICELOCATION = "http://www.rcsb.org/pdb/rest/search"


def post_query(xml):
    '''Post an XML query (PDB XML query format) to the RESTful
    RCSB web service

    Parameters
    ----------
    xml : str
       a string of xml query
    '''

    encodedXML = urllib.parse.quote(xml).encode('utf-8')

    url = urllib.request.Request(SERVICELOCATION)

    with urllib.request.urlopen(url, data=encodedXML) as f:

        pdbIds = [str(l)[2:-3] for l in f.readlines()]

    return pdbIds


def post_query_new(query):
    '''Post a JSON query (RCSB PDB JSON query format) to the RESTful
    RCSB web service

    Parameters
    ----------
    query : str
       a string of JSON query
    '''

    encodedJSON = urllib.parse.quote(query).encode('utf-8')

    url = urllib.request.Request(NEW_SERVICELOCATION)

    ids = []
    
    with urllib.request.urlopen(url, data=encodedJSON) as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith('"identifier"'):
                identifier = line.split(':')[1]
                identifier = identifier.strip()
                identifier = identifier[1:len(identifier)-2]
                ids.append[identifier]
                
    return ids
