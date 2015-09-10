from __future__ import absolute_import
from ..db.medgen import MedGenDB
from ..vocab import Vocab

###########################################################################
#
#       Type
#
###########################################################################

class Concept:
    """
    MedGen
    The NCBI Handbook [Internet]. 2nd edition.
    http://www.ncbi.nlm.nih.gov/books/NBK159970/
    """
    def __init__(self, concept):
        """
        MedGen concept
        :param concept: either umls_cui (UMLS unique concept) or medgen_uid (MedGen defined ID)
        :return: concept with both UMLS and MedGen defined IDs available.
        """
        try:
            self.umls_cui  = None
            self.medgen_uid= None

            if concept:
                self.medgen_uid = int(concept)
                self.umls_cui= str(MedGenDB().medgen2umls(concept))

        except ValueError:
            self.umls_cui  = str(concept)
            self.medgen_uid= int(MedGenDB().umls2medgen(concept))

    def __eq__(self, other):
        return (self.__dict__ == other.__dict__)

    def __str__(self):
        return str([
            str(Vocab("UMLS:ConceptID", self.umls_cui)),
            str(Vocab("MedGen:MedGenUID", self.medgen_uid))
        ])