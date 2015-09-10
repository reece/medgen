NCBI_GeneID =   "NCBI:GeneID"
HGNC_GeneName = "HGNC:Symbol"
UMLS_ConceptID  = "UMLS:CUI"
MedGen_ConceptID= "MedGen:UID"

class Vocab:
    """
    Annotator Vocabulary source and type of concept.

    examples: ncbi:GeneID, hgnc:GeneName, snomed:ClinicalTerm
    """
    def __init__(self, qualified_label, entry=None):

        self.qualified_label = qualified_label
        self.entry = entry

    def __str__(self):
        if self.entry:
            return str({self.qualified_label:self.entry})
        else:
            return str(self.qualified_label)