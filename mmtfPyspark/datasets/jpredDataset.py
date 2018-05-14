'''jpredDataset.py

This class downloads the dataset used to train the secondary structure predictor.
It can be used as a reference dataset for machine learning applications.

This dataset includes the ScopID, sequence, DSSP secondary structure assignment,
and a flag that indicates if data point was part of the training set.

References
----------
- `JPred4 <http://www.compbio.dundee.ac.uk/jpred/about_RETR_JNetv231_details.shtml>`_

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import urllib.request
import tarfile
from pyspark.sql import Row
from pyspark import SparkContext
from mmtfPyspark.ml import pythonRDDToDataset


def get_dataset():
    '''Gets JPred 4/JNet (v.2.3.1) secondary structure dataset.

    Returns
    -------
    dataset
       secondaryStructure dataset
    '''

    URL = "http://www.compbio.dundee.ac.uk/jpred/downloads/retr231.tar.gz"
    instream = urllib.request.urlopen(URL)
    secondaryStructures, sequences, trained = {}, {}, {}
    scopIds = set()
    res = []

    with tarfile.open(fileobj=instream, mode="r:gz") as tf:

        for entry in tf:
            if entry.isdir():
                continue
            br = tf.extractfile(entry)

            if ".dssp" in entry.name:
                scopID = str(br.readline())[3:-3]  # Remove newline and byte
                secondaryStructure = str(br.readline())[2:-3]  # Remove newline and byte
                secondaryStructure = secondaryStructure.replace('-', 'C')
                secondaryStructures[scopID] = secondaryStructure

            if ".fasta" in entry.name:
                scopID = str(br.readline())[3:-3]  # Remove newline and byte
                sequence = str(br.readline())[2:-3]  # Remove newline and byte
                scopIds.add(scopID)
                sequences[scopID] = sequence

                if "training/" in entry.name:
                    trained[scopID] = "true"
                elif "blind/" in entry.name:
                    trained[scopID] = "false"

    for scopId in scopIds:
        row = Row(scopId, sequences[scopId],
                  secondaryStructures[scopId], trained[scopId])
        res.append(row)

    sc = SparkContext.getOrCreate()
    data = sc.parallelize(res)
    colNames = ["scopID", "sequence", "secondaryStructure", "trained"]

    return pythonRDDToDataset.get_dataset(data, colNames)
