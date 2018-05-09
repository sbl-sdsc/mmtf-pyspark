#!/user/bin/env python
'''containsSequenceRegex.py:

This filter returns true if the polymer sequence motif matches the specified regular expression.
Sequence motifs support the following one-letter codes:
- 20 standard amino acids,
- O for Pyrrolysine,
- U for Selenocysteine,
- X for non-standard amino acid

References
----------
- Sequence motif: https://en.wikipedia.org/wiki/Sequence_motif

Examples
--------
Short sequence fragment -- NPPTP:
    The motif search supports wildcard queries by placing a '.' at the
    variable residue position. A query for an SH3 domains using the
    consequence sequence -X-P-P-X-P (where X is a variable residue and P is
    Proline),can be expressed as: .PP.P

Ranges of variable residues are specified by the {n} notation, where n is
the number of variable residues. To query a motif with seven variables
between residues W and G and twenty variable residues between G and L use
the following notation:
    W.{7}G.{20}L

Variable ranges are expressed by the {n,m} notation, where n is the minimum
and m the maximum number of repetitions. For example the zinc finger motif
that binds Zn in a DNA-binding domain can be expressed as:
    C.{2,4}C.{12}H.{3,5}H

The '^' operator searches for sequence motifs at the beginning of a protein
sequence. The following two queries find sequences with N-terminal Histidine
tags:
    ^HHHHHH or ^H{6}

Square brackets specify alternative residues at a particular position.
The Walker (P loop) motif that binds ATP or GTP can be expressed as:
    [AG].{4}GK[ST]
    A or G are followed by 4 variable residues, then G and K, and finally
    S or T

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

import re


class ContainsSequenceRegex(object):
    '''This filter returns true if the polymer sequence motif matches the
    specified regular expression.

    Attributes
    ----------
    regularExpression : str
       The regular expression of protein sequence
    '''

    def __init__(self, regularExpression):
        self.regex = regularExpression

    def __call__(self, t):
        structure = t[1]
        entity_list = [b['sequence'] for b in structure.entity_list]

        # This filter passes only single chains and the sequence cannot be empty
        for entity in entity_list:
            if len(entity) > 0:
                if len(re.findall(self.regex, entity)) > 0:
                    return True
        return False
