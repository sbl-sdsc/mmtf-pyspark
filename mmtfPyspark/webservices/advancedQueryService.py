#!/user/bin/env python
'''advancedQueryService.py

Post a JSON query using the RCSB PDB Search API 
'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import requests 

SERVICELOCATION = 'https://search.rcsb.org/rcsbsearch/v1/query'

def post_query(query):
    '''Post a JSON query (RCSB PDB JSON query format) to the RESTful
    RCSB web service (see: https://search.rcsb.org/index.html#search-api).

    Parameters
    ----------
    query : str
       a string of JSON query
    '''

    response = requests.post(SERVICELOCATION, data=query).json()

    if response is None:
        return []

    result_type = response['result_type']
    identifiers = [result['identifier'] for result in response['result_set']]

    return identifiers
