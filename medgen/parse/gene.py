from __future__ import absolute_import

from ..db.gene import GeneDB
from ..vocab import Vocab, NCBI_GeneID, HGNC_GeneName

###########################################################################
#
#       Type
#
###########################################################################

class Gene:
    def __init__(self, gene):
        """
         Gene defines both the Entrez GeneID and Hugo GeneName.
        :param gene: gene_id (GeneID) or hgnc (GeneName)
        :return:
        """
        try:
            self.id = None
            self.name = None

            if gene:
                self.id = int(gene)
                self.name = GeneDB().get_gene_name(gene)

        except ValueError:
            self.name = gene
            self.id = GeneDB().get_gene_id_for_gene_name(gene)

    def __eq__(self, other):
        return (self.__dict__ == other.__dict__)

    def __str__(self):
        return str([
            str(Vocab(NCBI_GeneID, str(self.id))),
            str(Vocab(HGNC_GeneName, self.name))
        ])
