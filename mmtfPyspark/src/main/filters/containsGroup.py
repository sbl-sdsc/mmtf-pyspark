#!/user/bin/env python
'''
containsGroup.py:

This filter returns entries that contain specified groups (residues).
Groups are specified by their one, two, or three-letter codes, e.g. "F", "MG", "ATP", as defined
in the <a href="https://www.wwpdb.org/data/ccd">wwPDB Chemical Component Dictionary</a>.

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

class containsGroup(object):
    '''
    Attributes:
        groupQuery (list[str]): list of group names
    '''
    def __init__(self, *args):
        groups = [a for a in args]
        self.groupQuery = set(groups)


    def __call__(self,t):
        groups = [t[1].group_list[idx]['groupName'] for idx in t[1].group_type_list]

        for group in self.groupQuery:
            if group not in groups:
                return False

        return True
