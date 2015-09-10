from __future__ import absolute_import

from .dataset import SQLData
from ..parse.concept import Concept
from ..parse.gene import Gene
from ..log import log, IS_DEBUG_ENABLED

##########################################################################################
#
#       SQLData Class
#
##########################################################################################

class PersonalGenomesDB(SQLData):
    """
    PGP contains annotations using BioNotate.
    Contributors welcome!
    """

    def __init__(self):
        super(PersonalGenomesDB, self).__init__(config_section='personalgenomes')

    def bionotate__gene_aa_pos(self, hgnc, variant_aa_pos):
        """
        human curated variants from bionotate / Personal Genomes Project
        :param hgnc: string hugo gene name
        :param variant_aa_pos: amino acid position
        :return:
        """
        _sql = """
        SELECT distinct
          PMID, 
          variant_gene as gene,
          variant_id   as vid,
          variant_aa_del,
          variant_aa_pos ,
          variant_aa_ins
        FROM   bionotate
        WHERE  gene = '?gene' and variant_aa_pos = '?pos'
        """
        return self.fetchall(_sql.replace('?gene', hgnc).replace('?pos', variant_aa_pos))


