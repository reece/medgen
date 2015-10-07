# from __future__ import absolute_import
##########################################################################################
from ..db.medgen     import MedGenDB
from ..db.clinvar    import ClinVarDB
from ..parse.concept import Concept


##########################################################################################
#
#       API
#
##########################################################################################

DiseaseName     = ClinVarDB().disease_name
DiseaseSubtypes = MedGenDB().disease_subtypes
DiseaseParents  = MedGenDB().disease_parents