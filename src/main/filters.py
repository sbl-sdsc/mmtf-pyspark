#!/user/bin/env python
'''
filters.py:
This file contains all the filter functions that can be used for Spark's filter operation

Authorship information:
    __author__ = "Peter Rose"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmai.com:
    __status__ = "debug"
TODO:
    Debug and Test
    Doc String
'''
import re

class rWork(object):
    '''This filter return true if the r_work value for this structure is within the specified range.

    Attributes:
        min_Rwork (float): The lower bound r_work value
        max_Rwork (float): The upper bound r_work value
    '''
    def __init__(self, minRwork, maxRwork):
        '''The class initalizer that assigns the argumentss to the attributes

        Args:
            minRwork (float): The lower bound r_work value
            maxRwork (float): The upper bound r_work value
        '''
        self.min_Rwork = minRwork
        self.max_Rwork = maxRwork
    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:
            t (SparkRDD): The RDD for PDB protein database
        Returns:
            bool: True for within range, False if MMTFDecoder doesn't have r_work or out of range
        '''
        if t[1].r_work == None:
            return False
        return t[1].r_work >= self.min_Rwork and t[1].r_work <= self.max_Rwork


class rFree(object):
    '''This filter return true if the r_free value for this structure is within the specified range.

    Attributes:
        min_Rfree (float): The lower bound r_free value
        max_Rfree (float): The upper bound r_free value
    '''
    def __init__(self, minRfree, maxRfree):
        '''The class initalizer that assigns the argumentss to the attributes

        Args:
            minRfree (float): The lower bound r_free value
            maxRfree (float): The upper bound r_free value
        '''
        self.min_Rfree = minRfree
        self.max_Rfree = maxRfree
    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:
            t (SparkRDD): The RDD for PDB protein database
        Returns:
            bool: True for within range, False if MMTFDecoder doesn't have r_free or out of range
        '''
        if t[1].r_free == None:
            return False
        return t[1].r_free >= self.min_Rfree and t[1].r_free <= self.max_Rfree


class resolution(object):
    '''This filter return true if the resolution value for this structure is within the specified range.

    Attributes:
        min_resolution (float): The lower bound resolution
        max_resolution (float): The upper bound resolution
    '''
    def __init__(self, minResolution, maxResolution):
        '''The class initalizer that assigns the argumentss to the attributes

        Args:
            minResolution (float): The lower bound resolution
            maxResolution (float): The upper bound resolution
        '''
        self.min_Resolution = minResolution
        self.max_Resolution = maxResolution
    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:
            t (SparkRDD): The RDD for PDB protein database
        Returns:
            bool: True for within range, False if MMTFDecoder doesn't have resolution or out of range
        '''
        if t[1].resolution == None:
            return False
        return t[1].resolution >= self.min_Resolution and t[1].resolution <= self.max_Resolution


class experimentalMethods(object):
    '''This filter returns true if all the specified experimental methods match a PDB entry.
	Currently, the following experimental method types are supported:
  	"ELECTRON CRYSTALLOGRAPHY"
  	"ELECTRON MICROSCOPY"
 	"EPR"
 	"FIBER DIFFRACTION"
 	"FLUORESCENCE TRANSFER"
 	"INFRARED SPECTROSCOPY"
 	"NEUTRON DIFFRACTION"
 	"POWDER DIFFRACTION"
 	"SOLID-STATE NMR"
 	"SOLUTION NMR"
 	"SOLUTION SCATTERING"
 	"THEORETICAL MODEL" (note, the PDB does not contain theoretical models)
 	"X-RAY DIFFRACTION"

 	The current list of supported experimental method types can be found here:
 	"http://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v40.dic/Items/_exptl.method.html"

    Attributes:
        experimental_methods (list(string)): A list of experimental methods to check
    '''

    # constants to be used as arguments to the Experimental Methods filter
    ELECTRON_CRYSTALLOGRAPHY = "ELECTRON CRYSTALLOGRAPHY"
    ELECTRON_MICROSCOPY = "ELECTRON MICROSCOPY"
    ERP = "EPR"
    FIBER_DIFFRACTION = "FIBER DIFFRACTION"
    FLUORESCENCE_TRANSFER = "FLUORESCENCE TRANSFER"
    INFRARED_SPECTROSCOPY = "INFRARED SPECTROSCOPY"
    NEUTRON_DIFFRACTION = "NEUTRON DIFFRACTION"
    POWDER_DIFFRACTION = "POWDER DIFFRACTION"
    SOLID_STATE_NMR = "SOLID-STATE NMR"
    SOLUTION_NMR = "SOLUTION NMR"
    SOLUTION_SCATTERING = "SOLUTION SCATTERING"
    THEORETICAL_MODEL = "THEORETICAL MODEL"
    X_RAY_DIFFRACTION = "X-RAY DIFFRACTION"

    def __init__(self, *experimentalMethods):
        '''
        Args:
	    *experimentalMethods (list(string)) : A list of experimental methods to check
        '''
        self.experimental_methods = sorted(list(experimentalMethods))

    def __call__(self,t):
        '''
        Args:
	    t (SparkRDD): The RDD for PDB protein database
        Returns:
	    bool: True if the PDB entry has all the listed experimental methods, else False
        '''
        structure = t[1]
        if len(structure.experimental_methods) != len(self.experimental_methods):
            return False
        methods = sorted([b.decode().upper() for b in structure.experimental_methods])
        return methods == self.experimental_methods


# TODO NEED Debug/Testing, Not Sure if it is Working
class containsSequenceRegex(object):
    '''This filter returns true if the polymer sequence motif matches the specified regular expression.
	Sequence motifs support the following one-letter codes:
	20 standard amino acids,
	O for Pyrrolysine,
	U for Selenocysteine,
	X for non-standard amino acid
	TODO list nucleic acid codes here ...

	@see https://en.wikipedia.org/wiki/Sequence_motif

	Examples
	Short sequence fragment
	NPPTP
	The motif search supports wildcard queries by placing a '.' at the variable residue position.
        A query for an SH3 domains using the consequence sequence -X-P-P-X-P (where X is a variable residue and P is Proline),can be expressed as:
	.PP.P

	Ranges of variable residues are specified by the {n} notation, where n is the number of variable residues. To query a motif with seven variables between residues W and G and twenty variable residues between G and L use the following notation:
	W.{7}G.{20}L

	Variable ranges are expressed by the {n,m} notation, where n is the minimum and m the maximum number of repetitions. For example the zinc finger motif that binds Zn in a DNA-binding domain can be expressed as:
	C.{2,4}C.{12}H.{3,5}H

	The '^' operator searches for sequence motifs at the beginning of a protein sequence. The following two queries find sequences with N-terminal Histidine tags
	^HHHHHH or ^H{6}

	Square brackets specify alternative residues at a particular position. The Walker (P loop) motif that binds ATP or GTP can be expressed as:
	[AG].{4}GK[ST]
	A or G are followed by 4 variable residues, then G and K, and finally S or T
    Attributes:
    NOTE:
	- Ask about entity sequence, num_entities
	- Number 0 in enititySequence
    '''
    def __init__(self, regularExpression):
        '''
        Args:
        '''
        self.regex = regularExpression
    def __call__(self,t):
        '''
        Args:
	    t (SparkRDD): The RDD for PDB protein database
        Returns:
	    bool: Returns true if the polymer sequence motif matches the specified regular expression
        '''
        structure = t[1]
        entity_list = [b['sequence'] for b in structure.entity_list]
        #This filter passes only single chains and the sequence cannot be empty
        for entity in entity_list:
            if len(entity) > 0:
                if len(re.findall(self.regex,entity)) > 0 :
                    return True
        return False

#Double check with peter/ test


# TODO Can't Find getGroupNames : Double check with Peter if it is in group_list
class containsGroup(object):
    '''This filter returns entries that contain specified groups (residues).
    Groups are specified by their one, two, or three-letter codes, e.g. "F", "MG", "ATP", as definedin the wwPDB Chemical Component Dictionary (https://www.wwpdb.org/data/ccd).

    Attributes:
        groupQuery (list[str]): list of group names
    '''
    def __init__(self, *args):
        '''This constructor accepts a comma separated list of group names, e.g., "ATP","ADP"

        Args:
            groups (list[str]): list of group names
        '''
        groups = [a for a in args]
        self.groupQuery = set(groups)


    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:
            t (SparkRDD): The RDD for PDB protein database
        Returns:
            bool: True if RDD contains all groups listed, else False
        '''
        #TODO can't find getGroupName
        groups = [t[1].group_list[idx]['groupName'] for idx in t[1].group_type_list]
        for group in self.groupQuery:
            if group not in groups:
                return False
        return True


class containsPolymerChainType(object):
    '''This filter returns entries that contain chains made of the specified
    monomer types. The default constructor returns entries that contain at least
    one chain that matches the conditions. If the "exclusive" flag is set to true
    in the constructor, all chains must match the conditions. For a multi-model
    structure, this filter only checks the first model.


    Attributes:

    '''
    #default argument has to follow non-default argument
    def __init__(self, entity_Types, exclusive = False):
        '''The class

        Args:

        '''
        self.exclusive = exclusive
        self.entity_types = entity_Types

    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:

        Returns:
        '''
        structure = t[1]
        contrains_polymer = False
        global_match = False
        num_chains = structure.chains_per_model[0] #get number of chains in first model, nessary?
        #chain types in entity as key, enetity from entity_list
        group_counter = 0

        for i in range(num_chains):
            match = True
            chain_type = [chain['type'] for chain in structure.entity_list
                         if i in chain['chainIndexList']][0]
            polymer = chain_type == "polymer"

            if polymer:
                contains_polymer = True
            else:
                match = False
            #group_type_list

            for j in range(structure.groups_per_chain[i]):
                if match and polymer:
                    group_idx = structure.group_type_list[group_counter]
                    group_type = structure.group_list[group_idx]['chemCompType']
                    match = (group_type in self.entity_types)
                group_counter += 1

            if (polymer and match and not self.exclusive):
                return True

            if (polymer and not match and self.exclusive):
                return False

            if match:
                global_match = True

        return global_match and contains_polymer


class containsDProteinChain(object):
    '''This filter

    Attributes:

    '''
    def __init__(self, exclusive = False):
        '''The class

        Args:

    '''
        self.filter = containsPolymerChainType(["D-PEPTIDE LINKING", "PEPTIDE LINKING"], exclusive = exclusive)

    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:

        Returns:

        '''
        return self.filter(t)


class containsLProteinChain(object):
    '''This filter

    Attributes:

    '''
    def __init__(self, exclusive = False):
        '''The class

        Args:

    '''
        self.filter = containsPolymerChainType(["L-PEPTIDE LINKING", "PEPTIDE LINKING"], exclusive)

    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:

        Returns:

        '''
        return self.filter(t)


class containsRnaChain(object):
    '''This filter

    Attributes:

    '''
    def __init__(self, exclusive = False):
        '''The class

        Args:

    '''
        self.filter = containsPolymerChainType("RNA LINKING",exclusive)
    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:

        Returns:

        '''
        return self.filter(t)


class containsDnaChain(object):
    '''This filter

    Attributes:

    '''
    def __init__(self, exclusive = False):
        '''The class

        Args:

    '''
        self.filter = containsPolymerChainType("DNA LINKING", exclusive)
    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:

        Returns:

        '''
        return self.filter(t)


class containsDSaccharide(object):
    '''This filter

    Attributes:

    '''
    def __init__(self, exclusive = False):
        '''The class

            Args:
        '''
        self.filter = containsPolymerChainType(["D-SACCHARIDE", "SACCHARIDE",
                      "D-SACCHARIDE 1,4 AND 1,4 LINKING",
                      "D-SACCHARIDE 1,4 AND 1,6 LINKING"], exclusive = exclusive)

    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:

        Returns:

        '''
        return self.filter(t)


# TODO On hold: Make DsspSecondaryStucture class (Spark/utils/DsspSecondaryStructures)
class secondaryStructure(object):
    '''This Filter

    Attrbutes:
    '''
    def __init__(self,_):
        self.helixFractionMax = 1.0
        self.helixFractionMin = 0.0
        self.sheetFractionMax = 1.0
        self.sheetFractionMin = 0.0
        self.coilFractionMax = 1.0
        self.coilFractionMin = 0.0
    def __call__(self,t):
        structure = t[1]
        if len(structure.entity_list) == 1:
            if structure.sec_struct_list == 0 :
                return False
            helix = 0.0
            sheet = 0.0
            coil = 0.0
            #TODO Peter uses switch case, might have to do with getQ3Code
            #TODO Not sure how to DsspSecondaryStructure.getQ3Code
            #TODO double check break is for switchcase
            for code in structure.sec_struct_list:
                if code == "ALPHA_HELIX":
                    helix += 1
                if code == "EXTENDED":
                    sheet += 1
                if code == "COIL":
                    coil += 1
                else:
                    continue
            length = len(structure.sec_struct_list)
            helix /= length
            sheet /= length
            coil /= length
            return helix >= self.helixFractionMin and helix <= \
            self.helixFractionMax and sheet >= self.sheetFractionMin and sheet \
            <= self.sheetFractionMax and coil >= self.coilFractionMin and coil \
            <= self.coilFractionMax
        return False


class notFilter(object):
    '''This filter

    Attributes:
        Filter (function)
    '''
    def __init__(self, filter_function):
        '''The class initalizer that assigns the argumentss to the attributes

        Args:
        '''
        self.filter = filter_function
    def __call__(self,t):
        '''calling the rWorkFilter class as a function

        Args:
            t (SparkRDD): The RDD for PDB protein database
        Returns:
            bool: True for within range, False if MMTFDecoder doesn't have resolution or out of range
        '''
        return not self.filter(t)
