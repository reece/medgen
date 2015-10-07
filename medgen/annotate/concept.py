# from __future__ import absolute_import
##########################################################################################
from ..db.medgen import MedGenDB

##########################################################################################
#
#       Functions
#
##########################################################################################

def _medgen_url(cui):
    """
    URL linkout the NCBI MedGen website.
    :param cui: concept unique identifier
    :return: str url
    """
    return 'http://www.ncbi.nlm.nih.gov/medgen/' + str(cui)

def _define_medgen_concept(cui):
    """
    MedGen defined Concept Definitions.
    Source can be either UMLS or MedGen.
    UMLS definitions come from a variety of sources, such as SNOMED, MESH, etc.
    MedGen concept definitions are often from Office of Rare diseases, HPO terms, etc.

    http://www.ncbi.nlm.nih.gov/books/NBK159970/#_MedGen_Data_Model_

    :param cui: UMLS unique concept id
    :return: Definition string
    """
    try:
        concept_def = MedGenDB().concept_definition(cui)
        concept_def['url'] = _medgen_url(cui)

        return concept_def
    except:
        return None

##########################################################################################
#
#       API
#
##########################################################################################

ConceptName = MedGenDB().concept_name
ConceptDefinition  = _define_medgen_concept
ConceptRelations   = MedGenDB().concept_relations
ConceptSources     = MedGenDB().concept_sources
ConceptURL         = _medgen_url

# ALIAS
Define  = ConceptDefinition
Relate  = ConceptRelations