#!/user/bin/env python
'''
viewStructures.py

Simple wrapper functions that uses ipywidgets and py3Dmol to view a list of
protein structures.

Authorship information:
    __author__ = "Mars (Shih-Cheng) Huang"
    __maintainer__ = "Mars (Shih-Cheng) Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

from ipywidgets import interact
import py3Dmol

def simpleStructureViewer(pdbIds, style = 'cartoon', color = 'spectrum'):
    '''
    A wrapper function that simply displays a list of protein structures using
    ipywidgets and py3Dmol

    Attributes:
        pdbIds (list<Strings>) : A list of PDBIDs to display
        style : Style of 3D structure (stick line cross sphere cartoon VDW MS)
        color : Color of 3D structure
    '''

    if type(pdbIds) == str:
        pdbIds = [pdbIds]

    def view3d(i = 0):
        '''
        Simple structure viewer that uses py3Dmol to view PDB structure by
        indexing the list of PDBids

        Attributes:
            i (int): index of the protein if a list of PDBids
        '''

        print(f"PdbID: {pdbIds[i]}, Style: {style}")

        viewer = py3Dmol.view(query='pdb:'+pdbIds[i])
        viewer.setStyle({style: {'color': color}})

        return viewer.show()

    return interact(view3d, i=(0,len(pdbIds)-1))


def interactionStructureViewer(pdbIds, interacting_atom = 'None', style = 'cartoon', color = 'spectrum'):
    '''
    A wrapper function that simply displays a list of protein structures using
    ipywidgets and py3Dmol and highlight specified interacting groups

    Attributes:
        pdbIds (list<Strings>) : A list of PDBIDs to display
        interacting_atom (String) : The interacting atom to highlight
        style : Style of 3D structure (stick line cross sphere cartoon VDW MS)
        color : Color of 3D structure
    '''

    if type(pdbIds) == str:
        pdbIds = [pdbIds]

    def view3d(i = 0):
        '''
        Simple structure viewer that uses py3Dmol to view PDB structure by
        indexing the list of PDBids

        Attributes:
            i (int): index of the protein if a list of PDBids
        '''

        print(f"PdbID: {pdbIds[i]}, Interactions: {interacting_atom}, Style: {style}")

        viewer = py3Dmol.view(query='pdb:'+pdbIds[i])
        viewer.setStyle({style: {'color': color}})

        if interacting_atom != "None":

            viewer.setStyle({'atom': interacting_atom},{'sphere': {'color':'gray'}})

        return viewer.animate()

    return interact(view3d, i = (0,len(pdbIds)-1))



def groupNeighborViewer(pdbIds = None, groups = None, chains = None, distance = 3.0):
    '''
    A wrapper function that zooms in to a group of a protein structure and highlight
    its neighbors within a certain distance.

    Attributes:
        pdbIds (list<Strings>, String) : A list of PDBIDs to display
        groups (list<int>) : A list of groups to center at for each protein structure
        chains (list<char>) : A list of chains specified for each protein structure.
                              If no chains is specified, chain 'A' will be default to
                              all structures.
        cutoffDistance (float) : The cutoff distance use the find the neighbors
                                 of specified group
    '''

    if pdbIds == None or groups == None:
        raise ValueError("PdbIds and groups need to be specified")

    if len(pdbIds) != len(groups):
        raise ValueError("Number of structures should match with number of groups")

    if type(pdbIds) == str and groups == str:
        pdbIds, groups = [pdbIds], [groups]

    if chains == None:
        chains = ['A'] * len(pdbIds)

    def view3d(i = 0):
        '''
        Simple structure viewer that zooms into a specified group and highlight
        its neighbors

        Attributes:
            i (int): index of the protein if a list of PDBids
        '''

        print(f"PDB: {pdbIds[i]}, group: {groups[i]}, chain: {chains[i]}, cutoffDistance: {distance}")

        center = {'resi':groups[i],'chain':chains[i]}
        neighbors = {'resi':groups[i],'chain':chains[i],'byres':'true','expand': distance}

        viewer = py3Dmol.view(query='pdb:'+pdbIds[i])
        viewer.zoomTo(center)
        viewer.setStyle(neighbors,{'stick':{}});
        viewer.setStyle(center,{'sphere':{'color':'red'}})
        viewer.zoom(0.2, 1000);

        return viewer.show()

    return interact(view3d, i = (0,len(pdbIds)-1))
