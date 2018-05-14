#!/user/bin/env python
'''interactionFingerprinter.py

This class creates dataset of ligand - macromolecule and macromolecule -
macromolecule interaction information. Criteria to select interactions are
specified by the InteractionFilter.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext
from mmtfPyspark.interactions import LigandInteractionFingerprint, PolymerInteractionFingerprint


class InteractionFingerprinter(object):

    def get_ligand_polymer_interactions(structures, interactionFilter):
        '''Returns a dataset of ligand - macromolecule interacting residues.

        The dataset contains the following columns:
        - structureChainId - pdbId.chainName of chain that interacts with ligand
        - queryLigandId - id of ligand from PDB chemical component dictionary
        - queryLigandNumber - group number of ligand including insetion code
        - queryLigandChainId - chain name of ligand
        - targetChainId - name of chain for which the interaction data are listed
        - groupNumbers - array of residue number of interacting groups including insertion code (e.g. 101A)
        - sequenceIndices - array of zero-based index of interaction groups (residues) mapped onto target sequence
        - sequence - interacting polymer sequence
        - interactingChains - total number of chains that interact with ligand

        Parameters
        ----------
        structures : PythonRDD
           a set of PDB structures
        interactionFilter : InteractionFilter
           interaction criteria

        Returns
        -------
        dataset
           dataset with interacting residue information
        '''

        # find sll interactions
        row = structures.flatMap(LigandInteractionFingerprint(interactionFilter))

        # convert RDD to a Dataset with the following columns
        nullable = False
        fields = [StructField("structureChainId", StringType(), nullable),
                  StructField("queryLigandId", StringType(), nullable),
                  StructField("queryLigandNumber", StringType(), nullable),
                  StructField("queryLigandChainId", StringType(), nullable),
                  StructField("targetChainId", StringType(), nullable),
                  StructField("groupNumbers", ArrayType(StringType(), nullable), nullable),
                  StructField("sequenceIndices", ArrayType(IntegerType(), nullable), nullable),
                  StructField("sequence", StringType(), nullable),
                  StructField("interactingChains", IntegerType(), nullable)
                  ]

        schema = StructType(fields)
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(row, schema)


    def get_polymer_interactions(structures, interactionFilter):
        '''Returns a dataset of ligand - macromolecule interacting information.

        The dataset contains the following columns:
            structureChainId - pdbId.chainName for which the interaction data are listed
            queryChainId - name of chain that interacts with target chain
            targetChainId - name of chain for which the interaction data are listed
            groupNumbers - array of residue number of interacting groups including insertion code (e.g. 101A)
            sequenceIndices - array of zero-based index of interaction groups (residues) mapped onto target sequence
            sequence - target polymer sequence

        Parameters
        ----------
        structures : PythonRDD
           a set of PDB structures
        interactionFilter : InteractionFilter
           interaction criteria

        Returns
        -------
        dataset
           dataset with interacting residue information
        '''

        # find all interactions
        row = structures.flatMap(PolymerInteractionFingerprint(interactionFilter))

        # convert RDD to a Dataset with the following columns
        nullable = False
        fields = [StructField("structureChainId", StringType(), nullable),
                  StructField("queryChainId", StringType(), nullable),
                  StructField("targetChainId", StringType(), nullable),
                  StructField("groupNumbers", ArrayType(StringType(),nullable), nullable),
                  StructField("sequenceIndices", ArrayType(IntegerType(), nullable), nullable),
                  StructField("sequence", StringType(), nullable),
                  ]

        schema = StructType(fields)
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(row, schema)
