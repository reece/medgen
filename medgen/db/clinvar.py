# from __future__ import absolute_import

from .dataset import SQLData
from ..parse.concept import Concept
from ..parse.gene import Gene
from ..log import log, IS_DEBUG_ENABLED

##########################################################################################
#
#       SQLData Class
#
##########################################################################################

class ClinVarDB(SQLData):
    """
    ClinVar is the NCBI database of clinical variants.
    ClinVar links MedGen phenotypes to HGVS variants and PubMed articles
    """
    def __init__(self):
        super(ClinVarDB, self).__init__(config_section='clinvar')

    def clinvar_ids(self, hgvs_text, id_column='VariationID'):
        """
        Get clinvar identifier for HGVS text label.
        ClinVar XML contains all of the recognizable HGVS strings.
        See medgen-mysql for specifics on how hgvs_text and ClinVar identifiers are associated.

        :param hgvs_text: c.DNA, r.RNA, p.Protein, or g.Genomic
        :param id_column: 'VariationID', 'AlleleID', or 'RCVaccession'
        :return: clinvar identifer 'VariationID', 'AlleleID', or 'RCVaccession'
        """
        res = self.fetchall(
            " select distinct %s as ID " % id_column +
            " from clinvar_hgvs " +
            " where hgvs_text = '%s'" % hgvs_text)

        return [entry['ID'] for entry in res]

    def accession_for_hgvs_text(self, hgvs_text):
        """
        Get RCVaccession for hgvs_text
        :param hgvs_text: c.DNA, r.RNA, p.Protein, or g.Genomic
        :return: RCVaccession
        """
        return self.clinvar_ids(hgvs_text, 'RCVaccession')

    def allele_id_for_hgvs_text(self, hgvs_text):
        """
        Get AlleleID for hgvs_text
        :param hgvs_text: c.DNA, r.RNA, p.Protein, or g.Genomic
        :return: AlleleID
        """
        return self.clinvar_ids(hgvs_text, 'AlleleID')

    def variation_id_for_hgvs_text(self, hgvs_text):
        """
        Get VariationID, the NCBI preferred identifier in ClinVar.
        :param hgvs_text: c.DNA, r.RNA, p.Protein, or g.Genomic
        :return: VariationID
        """
        return self.clinvar_ids(hgvs_text, 'VariationID')

    def hgvs_text_for_variation_id(self, variation_id):
        """
        Get hgvs_text for the specified NCBI Variant
        :param variation_id: Identifier preferred by NCBI ClinVar for a given Variant
        :return: arry of hgvs_text like ['NM_000530.6:c.233C>A']
        """
        res = self.fetchall("select distinct hgvs_text as ID from clinvar_hgvs where VariationID = %s " % str(variation_id))

        return [entry['ID'] for entry in res]

    def variant_summary(self, hgvs_c, hgvs_r=None, hgvs_p=None):
        """
        Get summary of a variant using the HGVS text label.
        The NCBI preferred identifier is VariationID, so use that instead if possible.

        To get a list of VariantSynonyms to work with this function, see the hgvs-lexicon package.
        hgvs-lexicon Maps HGVS synonyms names. We search with these synonyms where possible.

        [ hgvs_c, hgvs_r, hgvs_p] = VariantSynonyms(hgvs_c)

       +----------------------+--------------+------+-----+---------+-------+
       | Field                | Type         | Null | Key | Default | Extra | Invitae
       +----------------------+--------------+------+-----+---------+-------+
       | AlleleID             | int(11)      | NO   | MUL | NULL    |       | We use AlleleID instead of accession(s) to attempt mapping uniquely to clinvar
       | variant_type         | varchar(50)  | NO   | MUL | NULL    |       |
       | variant_name         | text         | YES  |     | NULL    |       |
       | GeneID               | int(11)      | NO   | MUL | NULL    |       | medgen.parse.gene.GeneID
       | Symbol               | varchar(20)  | NO   | MUL | NULL    |       | medgen.parse.gene.GeneName
       | ClinicalSignificance | text         | YES  |     | NULL    |       |
       | rs                   | int(11)      | YES  | MUL | NULL    |       |
       | dbvar_nsv            | text         | YES  |     | NULL    |       |
       | RCVaccession         | text         | YES  |     | NULL    |       |
       | TestedInGTR          | char(1)      | YES  | MUL | NULL    |       | GTR  !!!
       | PhenotypeIDs         | text         | YES  |     | NULL    |       | @todo: parse
       | Origin               | text         | YES  |     | NULL    |       |
       | Assembly             | text         | YES  |     | NULL    |       |
       | Chromosome           | varchar(20)  | YES  |     | NULL    |       |
       | Start                | int(11)      | YES  |     | NULL    |       |
       | Stop                 | int(11)      | YES  |     | NULL    |       |
       | Cytogenetic          | text         | YES  |     | NULL    |       |
       | ReviewStatus         | text         | YES  |     | NULL    |       |
       | HGVS_c               | varchar(200) | YES  | MUL | NULL    |       | hgvs_lexicon.parse.Variant
       | HGVS_p               | varchar(200) | YES  | MUL | NULL    |       | hgvs_lexicon.parse.Variant
       | NumberSubmitters     | int(11)      | YES  |     | NULL    |       |
       | LastEvaluated        | text         | YES  |     | NULL    |       |
       | Guidelines           | text         | YES  |     | NULL    |       |
       | OtherIDs             | text         | YES  |     | NULL    |       |
       +----------------------+--------------+------+-----+---------+-------+

        :param hgvs_c: c.DNA variant
        :param hgvs_r: r.RNA variant
        :param hgvs_p: p.Protein variant
        :return: variant summary as described in the clinvar TSV download file.
        """
        if IS_DEBUG_ENABLED:
            log.debug('hgvs_c '+str(hgvs_c))
            log.debug('hgvs_r '+str(hgvs_r))
            log.debug('hgvs_p '+str(hgvs_p))

        _select = """
             Select distinct TestedInGTR,
                  HGVS_c, HGVS_p, variant_name,
                  GeneID, Symbol,
                  variant_type, ClinicalSignificance, PhenotypeIDs,
                  AlleleID, dbvar_nsv, rs,
                  NumberSubmitters """

        _from   = """ From variant_summary """

        _where  = """ Where   (HGVS_c = '@c') or
                              (HGVS_c = '@r') or
                              (HGVS_p = '@p') """

        _where = _where.\
            replace('@c', str(hgvs_c)).\
            replace('@r', str(hgvs_r)).\
            replace('@p', str(hgvs_p))

        _sql = ' '+ _select + _from + _where

        if IS_DEBUG_ENABLED:
            log.debug(str( _sql   ))

        return self.fetchall(_sql)

    def var_citations(self, hgvs_text):
        """
        Get citations for clinvar entries using an HGVS text label.

        +----------+-------------+-----------+------+-----------------+-------------+
        | AlleleID | VariationID | rs        | nsv  | citation_source | citation_id |
        +----------+-------------+-----------+------+-----------------+-------------+
        |    15041 |           2 | 397704705 |    0 | PubMed          |    20613862 |
        |    15041 |           2 | 397704705 |    0 | PubMed          |    20613862 |
        |    15042 |           3 | 397704709 |    0 | PubMed          |    20613862 |
        |    15042 |           3 | 397704709 |    0 | PubMed          |    20613862 |
        |    15043 |           4 | 150829393 |    0 | PubMed          |    12030328 |
        |    15043 |           4 | 150829393 |    0 | PubMed          |    20531441 |
        |    15043 |           4 | 150829393 |    0 | PubMed          |    12030328 |
        |    15043 |           4 | 150829393 |    0 | PubMed          |    20531441 |
        |    15044 |           5 | 267606829 |    0 | PubMed          |    20818383 |
        +----------+-------------+-----------+------+-----------------+-------------+

        :param hgvs_text: c.DNA, r.RNA, p.Protein, g.Genomic
        :return: citations from ClinVar
        """
        if isinstance(hgvs_text, basestring):
            hgvs_text = [hgvs_text]

        hgvs_clause = " or ".join(map(lambda x: " H.hgvs_text = '{}' ".format(x), hgvs_text))
        sql = "select C.citation_id, C.citation_source, H.RCVaccession, H.hgvs_text from clinvar_hgvs H, var_citations C where H.VariationID = C.VariationID and ({})".format(hgvs_clause)
        return self.fetchall(sql)


    def molecular_consequences(self, hgvs_text):
        """
         Get Molecular Consequences for hgvs variant.
         Not all clinvar variants have known consequences.
         Sequence Ontology provides standard variant consequence naming.

        +--------------------+--------------+------+-----+---------+-------+
        | Field              | Type         | Null | Key | Default | Extra |
        +--------------------+--------------+------+-----+---------+-------+
        | hgvs_text          | varchar(200) | YES  | MUL | NULL    |       |
        | SequenceOntologyID | varchar(20)  | YES  | MUL | NULL    |       |
        | Consequence        | varchar(100) | YES  | MUL | NULL    |       |
        +--------------------+--------------+------+-----+---------+-------+

        :param hgvs_text:
        :return:
        """
        return self.fetchall("select * from clinvar.molecular_consequences where hgvs_text='?'  ".replace("?", hgvs_text))

    def random_example_hgvs(self, num_examples=1):
        """
        Get random mRNA variants in table clinvar.molecular_consequences
        :param num_examples: how many hgvs strings do you want?
        :return: list of hgvs strings

        :param num_examples:
        :return: list of hgvs variants
        """
        return list(self.hgvs_text(
            'select distinct(hgvs_text) as hgvs_text '
            'from molecular_consequences '
            'where hgvs_text '
            'like "NM%" '
            'order by rand() '
            'limit ' + str(num_examples)))

    def disease_name(self, concept):
        """
        MedGen (including ClinVar) refers to this standard set of disease names.
        :param concept:  UMLS or MedGen concept ID
        :return: DiseaseName entry (dictionary, includes VocabSource)
        """
        return self.fetchall(
         "select * "
         "from disease_names "
         "where ConceptID = '%s'" % Concept(concept).umls_cui)

    #TODO: @nthmost: refactor with medgen-services
    def gene2condition(self, gene_id):
        """
        gene2condition is a MedGen linked source spanning MedGen, ClinVar, GTR, OMIM, and HPO.

        desc gene_condition_source_id
        +--------------+---------------+------+-----+---------+-------+
        | Field        | Type          | Null | Key | Default | Extra |
        +--------------+---------------+------+-----+---------+-------+
        | GeneID       | int(11)       | YES  | MUL | NULL    |       |
        | Symbol       | varchar(10)   | YES  | MUL | NULL    |       |
        | ConceptID    | varchar(20)   | YES  | MUL | NULL    |       |
        | DiseaseName  | varchar(1000) | YES  | MUL | NULL    |       |
        | SourceName   | varchar(150)  | YES  | MUL | NULL    |       |
        | SourceID     | varchar(50)   | YES  | MUL | NULL    |       |
        | DiseaseMIM   | varchar(20)   | YES  | MUL | NULL    |       |
        | LastModified | varchar(20)   | YES  |     | NULL    |       |
        +--------------+---------------+------+-----+---------+-------+

        :param gene_id: Entrez Gene ID
        :return: condition information from gene_condition_source_id
        """
        return self.fetchall(
            " select * from gene_condition_source_id where GeneID = ?".replace('?',str(Gene(gene_id).id)))

    def gene2condition_for_concept(self, concept_id):
        """
        See gene2condition. Input is a concept rather than Gene.
        :param concept_id: MedGen concept id (CUI)
        :return: MedGen linked entry
        """
        return self.fetchall(
            " select * from gene_condition_source_id where ConceptID = '?' ".replace('?',str(concept_id)))


    def gene_summary(self, gene):
        """
        Get ClinVar gene summary

        mysql> select * from gene_specific_summary limit 20 ;
        +-------------+-----------+-------------+---------+
        | Symbol      | GeneID    | Submissions | Alleles |
        +-------------+-----------+-------------+---------+
        | A1BG        |         1 |           6 |       6 |
        | A1CF        |     29974 |           7 |       7 |
        | A2M         |         2 |          14 |      11 |
        | A2ML1       |    144568 |          15 |      14 |
        | A3GALT2     |    127550 |           1 |       1 |
        | A4GALT      |     53947 |          14 |      14 |
        | A4GNT       |     51146 |           5 |       5 |
        | AAAS        |      8086 |          14 |      14 |
        | AACS        |     65985 |           8 |       8 |
        | AADAC       |        13 |           7 |       7 |
        +-------------+-----------+-------------+---------+

        :param gene: NCBI Gene ID or hugo gene name
        :return: dictionary with counts of Submissions and Alleles
        """
        return self.fetchrow("select * from gene_specific_summary where GeneID = ? limit 1".replace('?', str(Gene(gene).id)))


    def gene_to_clinical_significance_type_frequency(self, gene):
        """
        Get clinical significance for a gene.

        ftp://ftp.ncbi.nlm.nih.gov/pub/GTR/standard_terms/Clinical_significance.txt

        Pathogenic
        Likely pathogenic
        drug response
        confers sensitivity
        risk factor
        other
        association
        Uncertain significance
        Likely benign
        Benign
        protective
        not provided
        conflicting data from submitters

        :param gene: NCBI Gene ID or hugo gene name
        :return: dict containing ClinicalSignificance and the number of variants (cnt_variants)
        """
        gene = Gene(gene)
        return self.fetchall(" select ClinicalSignificance, count(*) as cnt_variants "
                             " from variant_summary "
                             " where GeneID = ? "
                             " group by ClinicalSignificance "
                             " order by cnt_variants    desc "
                             .replace('?', str(gene.id)) )


    # TODO: deprecated
    def select_clinvar_gene_list_for_cui(self, cui):
        """
        select ClinVar genes associated with MedGen CUI
        :param cui:
        :return:
        """
        return self.list_genes("select Symbol as gene_name from gene_condition_source_id where ConceptID = '%s' " % cui)
    def get_version(self):
        '''
        Get the ClinVar version
        :return: string with version, such as '2015-10-2'
        '''
        try:
            sql_query = "select version from version_info order by last_loaded desc limit 1"
            return self.fetchrow(sql_query)['version']
        except:
            log.debug('No version numbers found for ClinVar')
            return None

