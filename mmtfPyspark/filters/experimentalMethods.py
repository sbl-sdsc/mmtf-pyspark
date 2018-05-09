#!/user/bin/env python
'''experimentalMethods.py:

This filter returns ture if any of the specified experimental methods
match a PDB entry

References
----------
- The current list of support experimental method types can be found here: http://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v40.dic/Items/_exptl.method.html

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"


class ExperimentalMethods(object):
    '''This filter returns True if any of the specified experimental methods
    matched a PDB entry.

    Attributes
    ----------
    experimental_methods : list
       A list of experimental methods to check
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
        self.experimental_methods = experimentalMethods

    def __call__(self, t):
        structure = t[1]
        methods = [b.upper() for b in structure.experimental_methods]
        return sum([1 for m in self.experimental_methods if m in methods]) > 0
