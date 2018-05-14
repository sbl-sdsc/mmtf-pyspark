'''uniProt.py

This class downloads and reads UniProt sequence files in the FASTA format and
converts them to datasets.This module reads the following files:
- SWISS_PROT,
- TREMBL,
- UNIREF50,
- UNIREF90,
- UNIREF100.

References
----------
- UniProt downloads <http://www.uniprot.org/downloads>`_
- The datasets have the following columns: http://www.uniprot.org/help/fasta-headers

Examples
--------
Download, read, and save the SWISS_PROT dataset:

>>> ds = uniProt.get_dataset(UniProtDataset.SWISS_PROT)
>>> ds.printSchema()
>>> ds.show(5)
>>> ds.write().mode("overwrite").format("parquet").save(fileName)
'''

__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from collections import namedtuple
import tempfile
import urllib.request
import gzip
from pyspark.sql import SparkSession
from enum import Enum


baseUrl = "ftp://ftp.uniprot.org/pub/databases/uniprot/"

SWISS_PROT = baseUrl + "current_release/knowledgebase/complete/uniprot_sprot.fasta.gz"
TREMBL = baseUrl + "current_release/knowledgebase/complete/uniprot_trembl.fasta.gz"
UNIREF50 = baseUrl + "uniref/uniref50/uniref50.fasta.gz"
UNIREF90 = baseUrl + "uniref/uniref90/uniref90.fasta.gz"
UNIREF100 = baseUrl + "uniref/uniref100/uniref100.fasta.gz"


def _get_uniprot_dataset(dataType):
    '''
    Get Uniprot Dataset
    '''

    # Decalre string variables
    db, uniqueIdentifier, entryName, proteinName, organismName, geneName, \
        proteinExistence, sequenceVersion, sequence = '', '', '', '', '', '', '', '', ''
    firstLine = True

    # Make temporary file
    tempFile = tempfile.NamedTemporaryFile(delete=False)
    with open(tempFile.name, "w") as t:

        t.writelines(
            "db,uniqueIdentifier,entryName,proteinName,organismName,geneName,proteinExistence,sequenceVersion,sequence\n")

        inputStream = urllib.request.urlopen(dataType, timeout=6000)
        rd = gzip.GzipFile(fileobj=inputStream)

        for line in rd:  # TODO check rd output content after UNIPROT online

            line = str(line)[2:-3]

            if ">" in line:
                line = line.replace(",", ";")

                if not firstLine:
                    t.writelines(
                        f"{db},{uniqueIdentifier},{entryName},{proteinName},{organismName},{geneName},{proteinExistence},{sequenceVersion}, {sequence}\n".replace(' ', ''))

                firstLine = False
                sequence = ""
                tmp = line.split("|")  # TODO
                db = tmp[0]
                uniqueIdentifier = tmp[1]
                tmp = tmp[2]  # TODO

                if len(tmp.split(" OS=")) > 2:
                    length = len(tmp.split(" OS")[0]) + \
                        len(tmp.split(" OS")[1]) + 4
                    tmp = tmp[:length]

                # Set sequence version
                sv = tmp.split(" SV=")
                tmp = sv[0]
                sequenceVersion = sv[1] if len(sv) > 1 else ""

                # Set proteinExistence
                pe = tmp.split(" PE=")
                tmp = pe[0]
                proteinExistence = pe[1] if len(pe) > 1 else ""

                # Set GeneName
                ge = tmp.split(" GN=")
                tmp = ge[0]
                geneName = ge[1] if len(ge) > 1 else ""

                # Set organismName
                on = tmp.split(" OS=")
                tmp = on[0]
                organismName = on[1] if len(on) > 1 else ""

                entryName = tmp.split(" ")[0]

                proteinName = tmp[len(entryName) + 1:]
            else:
                sequence += line

    spark = SparkSession.builder.getOrCreate()

    dataset = spark.read \
                   .format("csv") \
                   .option("header", "true") \
                   .option("inferSchema", "true") \
                   .load(tempFile.name)
    return dataset


def _get_uniref_dataset(dataType):
    '''
    Get Uniref Dataset
    '''

    # Decalre string variables
    uniqueIdentifier, clusterName, taxon, representativeMember, taxonID, \
        members = '', '', '', '', '', ''
    firstLine = True

    # Make temporary file
    tempFile = tempfile.NamedTemporaryFile(delete=False)
    with open(tempFile.name, "w") as t:

        t.writelines(
            "uniqueIdentifier,clusterName,members,taxon,taxonID,representativeMember,sequence\n")

        inputStream = urllib.request.urlopen(dataType)
        rd = gzip.GzipFile(fileobj=inputStream)

        for line in rd:  # TODO check rd output content after UNIPROT online

            line = str(line)[2:-3]

            if ">" in line:

                line = line.replace(",", ";")

                if not firstLine:
                    t.writelines(
                        f"{uniqueIdentifier},{clusterName},{members},{taxon},{taxonID},{representativeMember},{sequence}\n".replace(' ', ''))

                firstLine = False

                sequence = ""
                tmp = line

                # Set representativeMember
                rm = tmp.split(" RepID=")
                tmp = rm[0]
                representativeMember = rm[1] if len(rm) > 1 else ""

                # Set taxonID
                tid = tmp.split(" TaxID=")
                tmp = tid[0]
                taxonID = tid[1] if len(tid) > 1 else ""

                # Set taxon
                tx = tmp.split(" Tax=")
                tmp = tx[0]
                taxon = tx[1] if len(tx) > 1 else ""

                # Set members
                m = tmp.split(" n=")
                tmp = m[0]
                members = m[1] if len(m) > 1 else ""

                uniqueIdentifier = tmp.split(" ")[0]
                clusterName = tmp[len(uniqueIdentifier) + 1:]
            else:
                sequence += line

    spark = SparkSession.builder.getOrCreate()

    dataset = spark.read \
                   .format("csv") \
                   .option("header", "true") \
                   .option("inferSchema", "true") \
                   .load(tempFile.name)
    return dataset


def get_dataset(UniProtDataset):
    '''Returns the specified UniProt dataset.

    Parameters
    ----------
    uniProtDataset : str
       name of the UniProt dataset

    Returns
    -------
    dataset
       dataset with sequence and metadata
    '''

    if UniProtDataset.split('/')[-3] == "uniref":

        return _get_uniref_dataset(UniProtDataset)

    elif UniProtDataset.split('/')[-3] == "knowledgebase":

        return _get_uniprot_dataset(UniProtDataset)

    else:

        raise Exception("Please use pre-defined uniprotDataset \n \
                         eg: UniprotDataset.SWISS_PROT")
