#!/user/bin/env python
'''myVariantDataset.py

This class queries and retrieves missense variations using the MyVariant.info
web services for a list of UniProt ids.

References
----------
- For more information: http://myvariant.info
- Query syntax: http://myvariant.info/docs/
- Xin J, Mark A, Afrasiabi C, Tsueng G, Juchler M, Gopal N, Stupp GS, Putman TE, Ainscough BJ, Griffith OL, Torkamani A, Whetzel PL, Mungall CJ, Mooney SD, Su AI, Wu C (2016) High-performance web services for querying gene and variant annotation. Genome Biology 17(1):1-7. https://doi.org/10.1186/s13059-016-0953-9


Examples
--------
Get all missense variations for a list of Uniprot Ids:

>>> uniprotIds = ['P15056']    # BRAF
>>> ds = MyVariantDataset.get_variations(uniprotIds)
>>> ds.show()

Return missense variations that match a query

>>> uniprotIds = ['P15056']    # BRAF
>>> query = "clinivar.rcv.clinical_significance:pathogenic"
...       + "OR linivar.rcv.clinical_significance:likely pathogenic"
>>> ds = MyVariantDataset.get_variations(uniprotIds, query)
>>> ds.show()
+-------------------+---------+
|        variationId|uniprotId|
+-------------------+---------+
|chr7:g.140454006G>T|   P15056|
|chr7:g.140453153A>T|   P15056|
|chr7:g.140477853C>A|   P15056|
+-------------------+---------+

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark import SparkContext
from io import BytesIO
import requests


MYVARIANT_QUERY_URL = "http://myvariant.info/v1/query?q="
MYVARIANT_SCROLL_URL = "http://myvariant.info/v1/query?scroll_id="


def get_variations(uniprotIds, query = ''):
    '''Returns a dataset of missense variabtions for a list of Uniprot Ids and a
    MyVariant.info query.

    References
    ----------
    query syntax http://myvariant.info/docs/

    Examples
    --------
    >>> uniprotIds = ['P15056']    # BRAF
    >>> query = "clinivar.rcv.clinical_significance:pathogenic"
    ...         + "OR linivar.rcv.clinical_significance:likely pathogenic"
    >>> ds = MyVariantDataset.get_variations(uniprotIds, query)

    Parameters
    ----------
    uniprotIds : list
       list of Uniprot Ids
    query : str
       MyVariant.info query string ['']

    Returns
    -------
    dataset
       dataset with variation Ids and Uniprot Ids or null if no data are found
    '''

    # Get spark context
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # Download data in paralle
    data = sc.parallelize(uniprotIds).flatMap(lambda x: _get_data(x, query))

    # Convert Python Rdd to dataframe
    dataframe = spark.read.json(data)

    # return null if dataset contains no results
    if 'hits' not in dataframe.columns:
        print("MyVariantDataset: no match found")
        return None

    return _flatten_dataframe(dataframe)


def _get_data(uniprotId, query):
    '''get data from MyVariant
    '''

    data = []

    # Create url
    url = MYVARIANT_QUERY_URL \
          + "snpeff.ann.effect:missense_variant AND dbnsfp.uniprot.acc:" \
          + uniprotId
    if query is not '':
        url += f" AND ({query})"
    url += "&fields=_id&fetch_all=true"
    url = url.replace(" ","%20")

    # Open url
    try:
        req = requests.get(url)
    except:
        print(f"WARNING: Counld not load data for {uniprotId}")
        return data
    inputStream = BytesIO(req.content)

    # read results
    # A maximum of 1000 records are returned per request
    # Additional records are retrived iteratively using the scrollId
    results = _read_results(inputStream)

    # check if hits results are empty
    if "[]" not in results:
        results = _add_uniprot_id(results, uniprotId)
        data.append(results)

        scrollId = _get_scroll_id(results)

        while scrollId is not None:
            url = MYVARIANT_SCROLL_URL + scrollId
            try:
                req = requests.get(url)
            except:
                print(f"WARNING: Counld not load data for {uniprotId}")
                continue

            inputStream = BytesIO(req.content)
            results = _read_results(inputStream)

            if "No results to return" not in results:
                results = _add_uniprot_id(results, uniprotId)
                data.append(results)
                scollId = _get_scroll_id(results)
            else:
                scrollId = None

    return data


def _get_scroll_id(results):
    '''Get the scroll id from results
    '''
    if "_scroll_id" in results:
        return results.split("\"")[3]
    return None


def _read_results(inputStream):
    '''Converts data read from input stream into a single line of text

    Parameters
    ----------
    inputStream
       input stream

    Returns
    -------
    str
       response
    '''

    sb = ''
    for line in inputStream:
        sb += line.decode()
    return sb


def _add_uniprot_id(line, uniprotId):
    ids = "\"uniprotId\":" + "\"" + uniprotId + "\"," + "\"hits\"";
    return line.replace("\"hits\"", ids);
    #ids = f"\"uniprotIds\":\"{uniprotId}\",\n      \"_id\""
    #return line.replace("\"_id\"", ids)


def _flatten_dataframe(df):
    return df.withColumn("variationId", explode(df.hits._id)) \
             .select(col("variationId"), col("uniprotId"))
