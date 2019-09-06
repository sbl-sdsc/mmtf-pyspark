from abc import ABC, abstractmethod


class AbstractStructure(ABC):

    # Structure properties
    @property
    @abstractmethod
    def structure_id(self):
        pass

    @property
    @abstractmethod
    def title(self):
        pass

    @property
    @abstractmethod
    def experimental_methods(self):
        pass

    @property
    @abstractmethod
    def resolution(self):
        pass

    @property
    @abstractmethod
    def r_free(self):
        pass

    @property
    @abstractmethod
    def r_work(self):
        pass

    @property
    @abstractmethod
    def unit_cell(self):
        pass

    @property
    @abstractmethod
    def space_group(self):
        pass

    @property
    @abstractmethod
    def deposition_date(self):
        pass

    @property
    @abstractmethod
    def release_date(self):
        pass

    @property
    @abstractmethod
    def num_atoms(self):
        pass

    @property
    @abstractmethod
    def num_bonds(self):
        pass

    @property
    @abstractmethod
    def num_groups(self):
        pass

    @property
    @abstractmethod
    def num_chains(self):
        pass

    @property
    @abstractmethod
    def num_models(self):
        pass

    @property
    @abstractmethod
    def mmtf_version(self):
        pass

    @property
    @abstractmethod
    def mmtf_producer(self):
        pass

    # atom properties

    @property
    @abstractmethod
    def chain_names(self):
        """ Array of chains names (PDB chains)"""
        pass

    @property
    @abstractmethod
    def chain_ids(self):
        """ Array of chains names (PDB chains)"""
        pass

    @property
    @abstractmethod
    def group_names(self):
        pass

    @property
    @abstractmethod
    def atom_names(self):
        pass

    # alt_loc_list
    @property
    @abstractmethod
    def altlocs(self):
        pass

    #x_coord_list
    @property
    @abstractmethod
    def xcoords(self):
        pass

    #y_coord_list
    @property
    @abstractmethod
    def ycoords(self):
        pass

    #z_coord_list
    @property
    @abstractmethod
    def zcoords(self):
        pass

    #occupancy_list
    @property
    @abstractmethod
    def occupancies(self):
        pass

    #b_factor_list
    @property
    @abstractmethod
    def b_factors(self):
        pass

    @property
    @abstractmethod
    def elements(self):
        pass

    @property
    @abstractmethod
    def polymer(self):
        pass

    @property
    @abstractmethod
    def atom_id(self):
        pass

    @property
    @abstractmethod
    def group_id(self):
        pass

    #chem_comp_type
    @property
    @abstractmethod
    def chem_comp_types(self):
        pass

    #codes
    @property
    @abstractmethod
    def one_letter_codes(self):
        pass

    #group_serials
    @property
    @abstractmethod
    def group_serials(self):
        pass

    #entity_type
    @property
    @abstractmethod
    def entity_types(self):
        pass

    @property
    @abstractmethod
    def entity_indices(self):
        pass

    @property
    @abstractmethod
    def sequence_positions(self):
        pass