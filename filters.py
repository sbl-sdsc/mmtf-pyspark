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
'''

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
    NOTE: 
        - Should we give error statements for incorrect input?
        - Convert to all caps?
    ''' 
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
        
import re
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
	The motif search supports wildcard queries by placing a '.' at the variable residue position. A query for an SH3 domains using the consequence sequence -X-P-P-X-P (where X is a variable residue and P is Proline) can be expressed as:
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
