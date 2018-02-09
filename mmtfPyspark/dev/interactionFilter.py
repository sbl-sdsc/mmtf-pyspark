#!/user/bin/env python
'''
interactionFilter.py

A filter to specify criteria for molecular interactions between a query and a
target within a macromolecular structure. The filter specifies criteria for the
query (e.g. a metal ion) and the target (e.g. amino acid redisudes).
Interaction criteria such as distance cutoff limit the nature of interactions to
be considered.

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "dev"
'''

import sys

class InteractionFilter(object):
    ''' A filter to specify criteria for molecular interactions between a query
    and a target
    '''

    def __init__(self, distanceCutoff = sys.float_info.max,
                 normalizedbFactorCutoff = sys.float_info.max,
                 minInteractions = 1, maxInteractions = 10):

        # Set interaction criteria
        self._distanceCutoff = distanceCutoff
        self._normalizedbFactorCutoff = normalizedbFactorCutoff
        self._minInteractions = minInteractions
        self._maxInteractions = maxInteractions

        # Query criteria
        self._queryGroups = None
        self._queryElements = None

        # Target criteria
        self._targetGroups = None
        self._targetElements = None
        self._prohibitedTargetGroups = None


    def get_distance_cutoff(self):
        '''
        Get the maximum interaction distance. At least one pair of query and
        target atoms must be wintin in distance cutoff

        Returns:
            maximum interaction distance
        '''
        return self._distanceCutoff


    def set_distance_cutoff(self, distanceCutoff):
        '''
        Set the distnace cutoff for interacting atoms

        Attributes:
            distanceCutoff (float): the maximum distance for interacting atoms
        '''

        self._distanceCutoff = distanceCutoff


    def get_normalized_b_factor_cutoff(self):
        '''
        Gets the maximum normalized b-factor (z-score) cutoff for an atom and
        its interacting neighbor atoms.

        Returns:
            maximum normalized b-factor cutoff
        '''

        return self._normalizedbFactorCutoff


    def set_normalized_b_factor_cutoff(self, normalizedbFactorCutoff):
        '''
        Sets the maximum noramlized b-factor cutoff. This value represents a
        z-score (see reference below), the signed number of standard deviations
        by which the b-factor. High z-scores indicate either high flexibility
        and/or experimental error for the atoms involved in the interactions. By
        setting a cutoff value, not well defined interactions can be exculded.

        Frequently used z-scores:

        Confidence level    Tail area   z-scores
                     90%        0.05     +-1.645
                     95%        0.025    +-1.96
                     99%        0.005    +-2.576

        For example, to include all interactions within the 90% confidence
        interval, set the normalized b-factor to +1.645

        Attribute:
            normalizedbFactorCutoff (float): maximum normalized b-factor

        Reference:
            Z-score: https://en.wikipedia.org/wiki/Standard_score
        '''

        self._normalizedbFactorCutoff = normalizedbFactorCutoff


    def get_min_interactions(self):
        '''
        Returns the minimum number of interactions per atom. Atoms that interact
        with fewer atoms wil be discarded.

        Returns:
            minimum number of interactions per atom
        '''

        return self._minInteractions


    def set_min_interactions(self, minInteraction):
        '''
        Sets the minimum number of interactions per atom. Atoms that interact
        with fewer atoms will be discarded

        Attribute:
            minInteraction (int): minimum number of interactions per atom
        '''

        self._minInteractions = minInteraction


    def get_max_interactions(self):
        '''
        Returns the maximum number of interactions per atom. Atoms that interact
        with fewer atoms wil be discarded.

        Returns:
            maximum number of interactions per atom
        '''

        return self._maxInteractions


    def set_max_interactions(self, maxInteraction):
        '''
        Sets the maximum number of interactions per atom. Atoms that interact
        with fewer atoms will be discarded

        Attribute:
            maxInteraction (int): minimum number of interactions per atom
        '''

        self._maxInteractions = maxInteraction


    def set_query_elements(self, include, elements):
        '''
        Sets the elements to either be included or excluded in the query. Element
        strings are case sensitive (e.g., "Zn" for Zinc).

        Example: Only use elements O, N, S in the query groups in find polar
        interactions

            filter = InteractionFilter()
            filter.set_query_elements(True, ["O", "N", "S"])

        Example: Exclude non-polar elements and hydrogen in query groups and use
        all other elements to find interactions.

            elements = ['C', 'H', 'P']
            filter.set_query_elements(False, elements)

        Attributes:
            include (bool): if True, uses the specifed elements in the query,
                            if False, ignores the specified elemetns and use all
                            other elements
            elements (list): list of elements to be included or excluded in query
        '''

        if self._queryElements is not None:
            raise ValueError("ERROR: QueryElements have already been set.")

        self._includeQueryElements = include
        self._queryElements = set(elements)


    def set_target_elements(self, include, elements):
        '''
        Sets the elements to either be included or excluded in the target. Element
        strings are case sensitive (e.g., "Zn" for Zinc).

        Example: Only use elements O, N, S in the target groups in find polar
        interactions

            filter = InteractionFilter()
            filter.set_target_elements(True, ["O", "N", "S"])

        Example: Exclude non-polar elements and hydrogen in target groups and use
        all other elements to find interactions.

            elements = ['C', 'H', 'P']
            filter.set_query_elements(False, elements)

        Attributes:
            include (bool): if True, uses the specifed elements in the target,
                            if False, ignores the specified elemetns and use all
                            other elements
            elements (list): list of elements to be included or excluded in target
        '''

        if self._targetElements is not None:
            raise ValueError("ERROR: TargetElements have already been set.")

        self._includeTargetElements = include
        self._targetElements = set(elements)


    def set_query_groups(include, groups):
        '''
        Sets groups to either be included or excluded in the query. Group names
        must be upper case (e.g. 'ZN' for Zinc)

        Example: Find interactions with ATP and ADP

            filter = InteractionFilter()
            filter.set_query_groups(True, ['ATP', 'ADP'])

        Example: Exclude water and heavy water and use all other groups to find
        interactions.

            groups = ["HOH", "DOD"]
            filter.set_query_groups(False, groups)

        Attributes:
            include (bool): if True, uses the specified groups in the query,
                            if False, ignores the specified groups and uses all
                            other groups
            groups (list): groups to be included or excluded in query
        '''

        if self._queryGroups is not None:
            raise ValueError("ERROR: QueryGroups have already been set.")

        self._includeQueryGroups= include
        self._queryGroups = set(groups)


    def set_target_groups(include, groups):
        '''
        Sets groups to either be included or excluded in the target. Group names
        must be upper case (e.g. 'ZN' for Zinc)

        Example: Find interactions with specific amino acid groups.

            filter = InteractionFilter()
            filter.set_target_groups(True, ['CYS','HIS','ASP','GLU'])

        Example: Exclude water and heavy water and use all other groups to find
        interactions.

            groups = ["HOH", "DOD"]
            filter.set_target_groups(False, groups)

        Attributes:
            include (bool): if True, uses the specified groups in the target,
                            if False, ignores the specified groups and uses all
                            other groups
            groups (list): groups to be included or excluded in query
        '''

        if self._targetGroups is not None:
            raise ValueError("ERROR: QueryGroups have already been set.")

        self._includeTargetGroups= include
        self._targetGroups = set(groups)


    def set_prohibited_target_groups(groups):
        '''
        Sets groups that must not appear in interactions. Any interactions that
        involves the specified groups will be excluded from the results.

        Example Find Zinc interactions, but discard any interactions where the
        metal is involved in an interaction with water.

            filter = InteractionFilter()
            filter.set_query_groups(True, 'ZN')
            filter.set_prohibited_target_groups(["HOH"])

        Attributes:
            groups(list): one or more group names to be prohibited
        '''

        self._prohibitedTargetGroups = groups


    def is_query_element(element):
        '''
        Returns True if the specified elements matches the query conditions.

        Attributes:
            element (string): the element to be checked
        Returns:
            True if element matches query conditinos, else False
        '''

        if self._queryElements is None:
            return True

        if self._includeQueryElements:
            return element in self._queryElements
        else:
            return element not in self._queryElements


    def is_target_element(element):
        '''
        Returns True if the specified elements matches the target conditions.

        Attributes:
            element (string): the element to be checked
        Returns:
            True if element matches target conditinos, else False
        '''

        if self._targetElements is None:
            return True

        if self._includeTargetElements:
            return element in self._targetElements
        else:
            return element not in self._targetElements


    def is_query_group(group):
        '''
        Returns True if the specified group matches the query conditions.

        Attributes:
            group (string): the group to be checked
        Returns:
            True if element matches query conditinos, else False
        '''

        if self._queryGroups is None:
            return True

        if self._includeQueryGroups:
            return group in self._queryGroups
        else:
            return group not in self._queryGroups


    def is_target_group(group):
        '''
        Returns True if the specified group matches the target conditions.

        Attributes:
            group (string): the group to be checked
        Returns:
            True if element matches target conditinos, else False
        '''

        if self._targetGroups is None:
            return True

        if self._includeTargetGroups:
            return group in self._targetGroups
        else:
            return group not in self._targetGroups


    def is_prohibited_target_group(group):
        '''
        Returns True if the specified group must not occur in an interactions.

        Attributes:
            group (str): group that must not occur in interactions
        Returns:
            True if group is prohibited else False
        '''

        if self._prohibitedTargetGroups is None:
            return False

        return group in self._prohibitedTargetGroups
