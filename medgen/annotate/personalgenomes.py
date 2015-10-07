# from __future__ import absolute_import
##########################################################################################
from ..db.personalgenomes import PersonalGenomesDB

##########################################################################################
#
#       Functions
#
##########################################################################################

def _variant_to_bionotate(gene, amino_acid_position):
    return PersonalGenomesDB().bionotate__gene_aa_pos(gene, amino_acid_position)

##########################################################################################
#
#       API
#
##########################################################################################
Bionotate = _variant_to_bionotate
