from __future__ import absolute_import

from .dataset import SQLData
from ..parse.gene import Gene

##########################################################################################
#
#       SQLData Class
#
##########################################################################################

class HugoDB(SQLData):
    """
    HUGO gene names, info, and locus specific databases.
    """

    def __init__(self):
        super(HugoDB, self).__init__(config_section='hugo')

    def hugo_info(self, gene_symbol):
        """
        Get gene information, official records from the standard HUGO gene committee

        mysql> desc hugo_info;
        +------------------------+-------------+------+-----+---------+-------+
        | Field                  | Type        | Null | Key | Default | Extra |
        +------------------------+-------------+------+-----+---------+-------+
        | hgnc                   | varchar(25) | YES  |     | NULL    |       |
        | Symbol                 | varchar(50) | YES  |     | NULL    |       | variant2pubmed.parse.gene
        | Name                   | text        | YES  |     | NULL    |       |
        | Status                 | text        | YES  |     | NULL    |       |
        | LocusType              | text        | YES  |     | NULL    |       |
        | LocusGroup             | text        | YES  |     | NULL    |       |
        | PreviousSymbols        | text        | YES  |     | NULL    |       |
        | PreviousNames          | text        | YES  |     | NULL    |       |
        | Synonyms               | text        | YES  |     | NULL    |       |
        | NameSynonyms           | text        | YES  |     | NULL    |       |
        | Chromosome             | text        | YES  |     | NULL    |       |
        | DateNameChanged        | text        | YES  |     | NULL    |       |
        | AccessionNumbers       | text        | YES  |     | NULL    |       |
        | EnsemblGeneID          | int(10)     | YES  |     | NULL    |       |
        | SpecialistDB           | text        | YES  |     | NULL    |       |
        | pubmeds                | text        | YES  |     | NULL    |       | todo: check: same as gene2pubmed?
        | RefSeqIDs              | text        | YES  |     | NULL    |       |
        | GeneFamilyTag          | text        | YES  |     | NULL    |       |
        | RecordType             | text        | YES  |     | NULL    |       |
        | PrimaryIDs             | text        | YES  |     | NULL    |       |
        | LocusSpecificDatabases | text        | YES  |     | NULL    |       |
        +------------------------+-------------+------+-----+---------+-------+

        :param gene_symbol:
        :return:
        """
        return self.fetchall("select * from hugo.hugo_info where Symbol = '?' ".
                             replace('?', str(gene_symbol)))


    def get_locus_specific_databases(self, gene_symbol):
        """
        get LSDBs for a given gene symbol.

        :param gene_symbol:
        :return: dict {LocusSpecificDatabases, GeneFamilyTag, pubmeds}
        """
        gene = Gene(gene_symbol)

        return self.fetchrow(
            "select LocusSpecificDatabases, GeneFamilyTag, pubmeds "
            "from hugo_info where Symbol = '?' ".
            replace('?', str(gene.name)))