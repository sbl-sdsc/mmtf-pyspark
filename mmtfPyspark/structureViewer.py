#!/user/bin/env python
'''viewStructures.py

Simple wrapper functions that uses ipywidgets and py3Dmol to view a list of
protein structures.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from ipywidgets import interact, IntSlider, Dropdown
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import py3Dmol


def view_structure(pdbIds, bioAssembly = False, style='cartoon', color='spectrum'):
    '''A wrapper function that simply displays a list of protein structures using
    ipywidgets and py3Dmol

    Parameters
    ----------
    pdbIds : list
       A list of PDBIDs to display
    bioAssembly : bool
       display bioAssembly
    style : str, optional
       Style of 3D structure (stick line cross sphere cartoon VDW MS)
    color : str, optional
       Color of 3D structure

    '''
    if type(pdbIds) == str:
        pdbIds = [pdbIds]

    def view3d(i=0):
        '''Simple structure viewer that uses py3Dmol to view PDB structure by
        indexing the list of PDBids

        Parameters
        ----------
            i (int): index of the protein if a list of PDBids
        '''
        print(f"PdbID: {pdbIds[i]}, Style: {style}")


        if '.' not in pdbIds[i]:
            viewer = py3Dmol.view(query='pdb:' + pdbIds[i], options={'doAssembly': bioAssembly})
            viewer.setStyle({style: {'color': color}})
            viewer.setStyle({'hetflag': True},{'stick':{'singleBond':False}})

        else:
            pdbid,chainid = pdbIds[i].split('.')
            viewer = py3Dmol.view(query='pdb:' + pdbid, options={'doAssembly': bioAssembly})
            viewer.setStyle({})
            viewer.setStyle({'chain': chainid}, {style: {'color': color}})
            viewer.setStyle({'chain': chainid, 'hetflag': True},{'stick':{'singleBond':False}})
            viewer.zoomTo({'chain': chainid})

        return viewer.show()

    s_widget = IntSlider(min=0, max=len(pdbIds)-1, description='Structure', continuous_update=False)
    return interact(view3d, i=s_widget)


def view_group_interaction(pdbIds, interacting_group='None', style='cartoon', color='spectrum'):
    '''A wrapper function that simply displays a list of protein structures using
    ipywidgets and py3Dmol and highlight specified interacting groups

    Parameters
    ----------
    pdbIds : list
       A list of PDBIDs to display
    interacting_atom : str, optional
       The interacting atom to highlight
    style : str, optional
       Style of 3D structure (stick line cross sphere cartoon VDW MS)
    color : str, optional 
       Color of 3D structure

    '''
    if type(pdbIds) == str:
        pdbIds = [pdbIds]

    def view3d(i=0):
        '''Simple structure viewer that uses py3Dmol to view PDB structure by
        indexing the list of PDBids

        Parameters
        ----------
            i (int): index of the protein if a list of PDBids
        '''

        print(
            f"PdbID: {pdbIds[i]}, Interactions: {interacting_group}, Style: {style}")

        viewer = py3Dmol.view(query='pdb:' + pdbIds[i])
        viewer.setStyle({style: {'color': color}})

        if interacting_group != "None":

            viewer.setStyle({'resn': interacting_group}, {
                            'sphere': {}})

        return viewer.show()

    s_widget = IntSlider(min=0, max=len(pdbIds)-1, description='Structure', continuous_update=False)
    return interact(view3d, i=s_widget)


def view_binding_site(pdbIds=None, groups=None, chains=None, distance=3.0):
    '''A wrapper function that zooms in to a group of a protein structure and highlight
    its neighbors within a certain distance.

    Parameters
    ----------
    pdbIds : list, optional 
       A list of PDBIDs to display
    groups : list, optional
       A list of groups to center at for each protein structure
    chains : list, optional
       A list of chains specified for each protein structure.  
       If no chains is specified, chain 'A' will be default to
       all structures.
    cutoffDistance : float, optional
       The cutoff distance use the find the neighbors of specified group

    '''

    if pdbIds is None or groups is None:
        raise ValueError("PdbIds and groups need to be specified")

    if len(pdbIds) != len(groups):
        raise ValueError(
            "Number of structures should match with number of groups")

    if type(pdbIds) == str and groups == str:
        pdbIds, groups = [pdbIds], [groups]

    if chains is None:
        chains = ['A'] * len(pdbIds)

    def view3d(i=0):
        '''Simple structure viewer that zooms into a specified group and highlight
        its neighbors

        Parameters
        ----------
            i (int): index of the protein if a list of PDBids
        '''

        print(
            f"PDB: {pdbIds[i]}, group: {groups[i]}, chain: {chains[i]}, cutoffDistance: {distance}")

        if type(groups[i]) == int:
            center = {'resi': groups[i], 'chain': chains[i]}
            neighbors = {'resi': groups[i], 'chain': chains[i],
                         'byres': 'true', 'expand': distance}
        else:
            center = {'resn': groups[i], 'chain': chains[i]}
            neighbors = {'resn': groups[i], 'chain': chains[i],
                         'byres': 'true', 'expand': distance}


        viewer = py3Dmol.view(query='pdb:' + pdbIds[i])
        viewer.setStyle(neighbors, {'stick': {}});
        viewer.setStyle(center, {'sphere': {'colorscheme': 'orangeCarbon'}})
        viewer.zoomTo(neighbors)

        return viewer.show()

    s_widget = IntSlider(min=0, max=len(pdbIds)-1, description='Structure', continuous_update=False)
    return interact(view3d, i=s_widget)


def group_interaction_viewer(df, sortBy, metal=None):
    '''A wrapper function that zooms in to a group in a protein structure and
    highlight its interacting atoms. The input dataframe should be generated
    from the GroupInteractionExtractor class.

    References
    ----------
    GroupInteractionExtractor: https://github.com/sbl-sdsc/mmtf-pyspark/blob/master/mmtfPyspark/interactions/groupInteractionExtractor.py

    Parameters
    ----------
    df : dataframe
       the dataframe generated from GroupIneteractionExtractor
    sort_by : str
       the q value to sort by ['q4','q5','q6']

    '''

    # Filter by metal
    if metal is not None:
        df = df[df["element0"] == metal]

    # Sort dataframe based on sortBy parameter (q4-6 values)
    df = df.sort_values([sortBy], ascending = False).dropna(subset=[sortBy])
    if sortBy in ['q4','q5']:
        q = 'q' + str(int(sortBy[-1]) + 1)
        df = df[df[q] != np.nan]

    i_widget = IntSlider(
        min=0, max=df.shape[0] - 1, description='Structure', continuous_update=False)

    def get_neighbors_chain(i):
        return [df[f'chain{j}'].iloc[i] for j in range(1, 7) if df[f'element{j}'] is not None]

    def get_neighbors_group(i):
        return [df[f'groupNum{j}'].iloc[i] for j in range(1, 7) if df[f'element{j}'] is not None]

    def get_neighbors_elements(i):
        elements = [df[f'element{j}'].iloc[i]
                    for j in range(1, 7) if df[f'element{j}'] is not None]
        return [str(e).upper() for e in elements]

    def view3d(i=0):
        '''Simple structure viewer that uses py3Dmol to view PDB structure by
        indexing the list of PDBids

        Parameters
        ----------
        i : int
           index of the protein if a list of PDBids
        '''

        structures = df['pdbId'].iloc
        groups = df['groupNum0'].iloc
        chains = df['chain0'].iloc
        elements = df['element0'].iloc
        ori = str(df[sortBy].iloc[i])[:5]

        print(f"PDBId: {structures[i]}    chain: {chains[i]}    element:    {elements[i]}")
        print(f"{sortBy}: {ori}")

        viewer = py3Dmol.view(query='pdb:' + structures[i], width=700, height=700)
        neighbors = {'resi': get_neighbors_group(i), 'chain': get_neighbors_chain(i)}
        metal = {'resi': groups[i], 'atom': str(elements[i]).upper(), 'chain': chains[i]}

        viewer.setStyle(neighbors, {'stick': {'colorscheme': 'orangeCarbon'}})
        viewer.setStyle(metal, {'sphere': {'radius': 0.5, 'color': 'gray'}})

        viewer.zoomTo(neighbors)
        return viewer.show()

    return interact(view3d, i=i_widget)


def metal_distance_widget(df_concat):
    '''Plot an violinplot of metal-element distances with ipywidgets

    Parameters
    ----------
    df_concat : Dataframe
       dataframe of metal-elements distances

    '''
    metals = df_concat['Metal'].unique().tolist()
    m_widget = Dropdown(options = metals, description = "Metals")

    def metal_distance_violinplot(metal):
        df_metal = df_concat[df_concat["Metal"] == metal].copy()
        df_metal['Element'] = df_metal['Element'].apply(lambda x: metal+"-"+x)

        # Set fonts
        fig, ax = plt.subplots()
        fig.set_size_inches(15,6)
        subplot = sns.violinplot(x="Element", y="Distance", palette="muted", data=df_metal, ax=ax)
        subplot.set(xlabel="Metal Interactions", ylabel="Distance", title=f"{metal} to Elements Distances Violin Plot")

    return interact(metal_distance_violinplot, metal=m_widget);
