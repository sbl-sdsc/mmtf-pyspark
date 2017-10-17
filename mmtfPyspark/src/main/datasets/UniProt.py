'''
UniProt.py

Thisclass<ahref="http://www.uniprot.org/downloads">downloads</a>andreads
UniProtsequencefilesintheFASTAformatandconvertsthemtodatasets.This
classreadsthefollowingfiles:SWISS_PROT,TREMBL,UNIREF50,UNIREF90,UNIREF100.

Thedatasetshavethefollowing
<ahref="http://www.uniprot.org/help/fasta-headers">columns</a>.

<p>
Example:download,read,andsavetheSWISS_PROTdataset

<pre>
	{@code
	Dataset<Row>ds=UniProt.getDataset(UniProtDataset.SWISS_PROT);
	ds.printSchema();
	ds.show(5);
ds.write().mode("overwrite").format("parquet").save(fileName);
}
</pre>


Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

# TODO Uniprot Links are down, can't test

from collections import namedtuple
import tempfile
import urllib.request
import gzip
from pyspark.sql import SparkSession

baseUrl = "ftp://ftp.uniprot.org/pub/databases/uniprot/"

SWISS_PROT = baseUrl + "current_release/knowledgebase/complete/uniprot_sprot.fasta.gz"
TREMBL = baseUrl + "current_release/knowledgebase/complete/uniprot_trembl.fasta.gz"
UNIREF50 = baseUrl + "uniref/uniref50/uniref50.fasta.gz"
UNIREF90 = baseUrl + "uniref/uniref90/uniref90.fasta.gz"
UNIREF100 = baseUrl + "uniref/uniref100/uniref100.fasta.gz"

# TODO named tuple or enum
:
def getUniprotDataset(dataType):
    '''
    Get Uniprot Dataset
    '''

    # Decalre string variables
    db, uniqueIdentifier, entryName, proteinName, organismName, geneName, \
    proteinExistence, sequenceVersion, sequence = '','','','','','','','',''
    firstLine = True

    # Make temporary file
    tempFile = tempfile.NamedTemporaryFile(delete = False)
    with open (tempFile.name, "w") as t:

        t.writelines("db,uniqueIdentifier,entryName,proteinName, \
                     organismName,geneName,proteinExistence, \
                     sequenceVersion,sequence \n")

        inputStream = urllib.request.urlopen(dataType)
        rd = gzip.GzipFile(fileobj = inputStream)

        for line in rd: #TODO check rd output content after UNIPROT online

            if ">" in line:
                line = line.replace(",", ";")

                if not firstLine:
                    t.writelines(f"{db},{uniqueIdentifier},{entryName},\
                                 {proteinName},{organismName},{geneName},\
                                 {proteinExistence},{sequenceVersion}, \
                                 {sequence}\n")
                firstTime = False
                sequence = ""
                tmp = line[1].split("\\|") # TODO
                db = tmp[0]
                uniqueIdentifier = tmp[1]
                tmp[0] = tmp[2] # TODO

                if len(tmp[0].split(" OS=")) > 2 :
                    length = len(tmp[0].split(" OS")[0]) + \
                             len(tmp[0].split(" OS")[1]) + 4
                    tmp[0] = tmp[0][:length]

                # Set sequence version
                sv = tmp[0].split(" SV=")[-1]
                sequenceVersion = sv[1] if len(sv) > 1 else ""

                # Set proteinExistence
                pe = tmp[0].split(" PE=")[-1]
                proteinExistence = pe[1] if len(pe) > 1 else ""

                # Set GeneName
                gn = tmp[0].split(" GE=")[-1]
                geneName = ge[1] if len(ge) > 1 else ""

                # Set organismName
                on = tmp[0].split(" ON=")[-1]
                organismName = on[1] if len(on) > 1 else ""

                entryName = tmp[0].split(" ")[0]
                proteinName = tmp[0][len(entryName) + 1]
            else:
                sequence = sequence.concat(line) # TODO figure out what it is concating

    # TODO writelines again?

    spark = SparkSession.builder.getOrCreate()

    dataset = spark.read \
                   .format("csv")
                   .option("header","true")
                   .option("inferSchema", "true")
                   .load(tempFile.name)
    return dataset


def getUnirefDataset(dataType):
    '''
    Get Uniref Dataset
    '''

    # Decalre string variables
    uniqueIdentifier, clusterName, taxon, representativeMember, taxonID, \
    members = '','','','','',''
    firstLine = True

    # Make temporary file
    tempFile = tempfile.NamedTemporaryFile(delete = False)
    with open (tempFile.name, "w") as t:

        t.writelines("uniqueIdentifier,clusterName,members,taxon,taxonID,\
                     representativeMember,sequence\n")

        inputStream = urllib.request.urlopen(dataType)
        rd = gzip.GzipFile(fileobj = inputStream)

        for line in rd: #TODO check rd output content after UNIPROT online

            if ">" in line:
                line = line.replace(",", ";")

                if not firstLine:
                    t.writelines(f"{uniqueIdentifier},{clusterName},{members},\
                                 {taxon},{taxonID},{representativeMember},\
                                 {sequence}\n")
                firstTime = False

                sequence = ""
                tmp = line[1] # TODO

                # Set representativeMember
                rm = tmp.split(" RepID=")[-1]
                representativeMember = rm[1] if len(rm) > 1 else ""

                # Set taxonID
                tid = tmp.split(" TaxID=")[-1]
                taxonID = tid[1] if len(tid) > 1 else ""

                # Set taxon
                tx = tmp.split(" Tax=")[-1]
                taxon = tx[1] if len(tx) > 1 else ""

                # Set members
                m = tmp.split(" n=")[-1]
                members = m[1] if len(m) > 1 else ""

                uniqueIdentifier = tmp.split(" ")[0]
                clusterName = tmp[len(uniqueIdentifier) + 1]
            else:
                sequence = sequence.concat(line) # TODO figure out what it is concating

    # TODO writelines again?

    spark = SparkSession.builder.getOrCreate()

    dataset = spark.read \
                   .format("csv")
                   .option("header","true")
                   .option("inferSchema", "true")
                   .load(tempFile.name)
    return dataset
