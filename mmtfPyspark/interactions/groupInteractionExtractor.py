#!/user/bin/env python
'''groupInteractionExtractor.py

Creates a dataset of noncovalent interactions of specified groups (residues)
in macromolecular structures. The criteria for interactions are specified using
an InteractionFilter. The interactions can be returned as interacting atom pairs
or as one row per interacting atom.

Typical use cases include:
- Find interactions between a metal ion and protein/DNA/RNA
- Find interactions between a small molecule and protein/DNA/RNA

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "done"

from pyspark.sql import SparkSession
from pyspark import SparkContext
from mmtfPyspark.interactions import StructureToAtomInteractions, AtomInteraction


class GroupInteractionExtractor(object):

    def get_pair_interactions(self, structures, interactionFilter):
        '''Returns a Dataset of pairwise interactions that satisfy the criteria of
        the InteractionFilter. Each atom, its interacting neightbor atom, and
        the interacting distance is represented as a row.

        Parameters
        ----------
        structures : PythonRDD
           a set of PDB structures
        interactionFilter : InteractionFilter
           filter criteria for determing noncovalent interactions

        Returns
        -------
        dataset
           Dataset of pairwise interactions
        '''

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        # calculate interactions
        pairwise = True
        rows = structures.flatMap(StructureToAtomInteractions(
            sc.broadcast(interactionFilter), pairwise))

        # convert PythonRDD to Dataset
        return spark.createDataFrame(rows, AtomInteraction().get_pair_interaction_schema())

    def get_interactions(self, structures, interactionFilter):
        '''Returns a dataset of interactions that satisfy the criteria of the
        InteractionFilter. each atom and its interacting neightbor atoms are
        represented as a row in a Dataset. In addition, geometric freatures of
        the interactions, such as distances, angles, and orientation order
        parameters are returned in each row.

        Parameters
        ----------
        structures : PythonRDD
           a set of PDB structures
        interactionFilter : InteractionFilter
           filter criteria for determing noncovalent interactions

        Returns
        -------
        dataset
           Dataset of pairwise interactions
        '''

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        # calculate interactions
        pairwise = False
        rows = structures.flatMap(StructureToAtomInteractions(
            sc.broadcast(interactionFilter), pairwise))

        # convert PythonRDD to Dataset
        return spark.createDataFrame(rows, AtomInteraction().get_schema(interactionFilter.get_max_interactions()))
