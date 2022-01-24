#!/user/bin/env python
'''advancedQuery.py

This filter runs an RCSB PDB Advanced Search web service using an XML query
description.

References
----------
- `Advanced Search Query <https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedSearch.html>`_

Examples
--------
Find PDB entries that contain the word "mutant" in the structure title:

>>> query = "<orgPdbQuery>" + \
...         "<queryType>org.pdb.query.simple.StructTitleQuery</queryType>" + \
...         "<struct.title.comparator>contains</struct.title.comparator>" + \
...         "<struct.title.value>mutant</struct.title.value" + \
... "</orgPdbQuery>"
>>> pdb = pdb.filter(AdvancedSearch(query));

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from mmtfPyspark.webservices.advancedQueryService import post_query


class AdvancedQuery(object):
    '''Filters using the RCSB PDB Advanced Search web service

    Attributes
    ----------
    xmlQuery : str
       query in RCSB PDB XML format
    '''

    def __init__(self, xmlQuery):

        results = post_query(xmlQuery)

        self.entityLevel = (len(results) > 0) and (":" in results[0])
        self.structureIds = list(set(results))
        self.exclusive = False

    def __call__(self, t):

        structure = t[1]

        globalMatch = False
        numChains = structure.chains_per_model[0]
        entityChainIndex = self._get_chain_to_entity_index(structure)

        for i in range(numChains):

            ID = t[0]

            if self.entityLevel:
                ID = self._get_structure_entity_id(
                    structure, ID, entityChainIndex[i])

            match = ID in self.structureIds

            if match and not self.exclusive:
                return True

            if not match and self.exclusive:
                return False

            if match:
                globalMatch = True

        return globalMatch

    def _get_structure_entity_id(self, structure, origStructureId, origEntityId):

        keyStructureId = origStructureId

        try:
            index = keyStructureId.index(".")
            keyStructureId = keyStructureId[:index]
        except:
            pass

        try:
            pos = structure.structure_id.rindex(".")

            valueStructureId = structure.structure_id[:structure.structure_id.index(
                ".")]

            if keyStructureId != valueStructureId:
                raise Exception("Structure mismatch: key vs value: %s vs. %s"
                                % (keyStructureId, valueStructureId))

            entityId = structure.structure_id[pos + 1:]
            ID = valueStructureId + ":" + entityId

        except:
            ID = keyStructureId + ":" + str(origEntityId + 1)

        return ID

    def _get_chain_to_entity_index(self, structure):

        entityChainIndex = [0] * structure.num_chains

        for i in range(len(structure.entity_list)):

            for j in structure.entity_list[i]['chainIndexList']:

                entityChainIndex[j] = i

        return entityChainIndex
