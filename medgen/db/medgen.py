from __future__ import absolute_import
from .dataset import SQLData

################################################################################
#
# Formatting helper functions
#
################################################################################

def is_format_umls(concept='C0007194'):
    """
    CUI style notation
    :param concept:
    :return: boolean
    """
    return str(concept).startswith('C') and (8)== str(concept).__len__()

def is_format_medgen(concept=2881):
    """
    UID style notation
    :param concept:
    :return: boolean
    """
    try:
        concept = int(concept)
        return str(concept).__len__() <= (6)
    except ValueError:
        return False

##########################################################################################
#
#       SQLData Class
#
##########################################################################################

class MedGenDB(SQLData):
    """
    MedGen is the NCBI primary source for Medical Genetics.
    http://www.ncbi.nlm.nih.gov/books/NBK159970/
    https://bitbucket.org/invitae/medgen-mysql
    """

    def __init__(self):
        super(MedGenDB, self).__init__(config_section='medgen')

    def disease_subtypes(self, cui):
        """
        Narrower Hierarchical Relationship:
        Disease subtypes refers to an IS-A relationship in the medgen term hierarchy.
        :param cui: MedGen concept ID
        :return: id, name, and source of the disease concept
        """
        cui = self.get_concept_id(cui)
        select_template = '''
        select distinct
        SubtypeID     as DiseaseID,
        SubtypeName   as DiseaseName,
        SubtypeSource as DiseaseSource
        from view_disease_subtype where DiseaseID="%s";'''
        return self.fetchall(select_template % cui)

    def disease_parents(self, cui):
        """
        Broader Hierarchical Relationship:
        Disease parents refers to more general definitions of the disease specified by the cui param.
        This table is created by invitae, not downloaded directly from medgen.
        :param cui: MedGen concept
        :return: id, name, and source of the disease
        """
        cui = self.get_concept_id(cui)
        select_template = '''select distinct DiseaseID, DiseaseName, DiseaseSource from view_disease_subtype where SubTypeID="%s";'''
        return self.fetchall(select_template % cui)

    def concept_name(self, cui):
        """
        Get preferred concept name, NCBI MedGen first prefers GTR/ClinVar concept names, then SNOMED-CT, followed by MESH.
        Most concept names comes from SNOMED-CT (Clinical Terms)

        Example entry:
        +----------+--------------------------------------------------------+-------------+----------+
        | CUI      | name                                                   | source      | SUPPRESS |
        +----------+--------------------------------------------------------+-------------+----------+
        | C1997451 | History of repair of atrial septal defect              | SNOMEDCT_US | N        |
        +----------+--------------------------------------------------------+-------------+----------+

        :param cui: medgen concept
        :return: conept name
        """
        return self.fetchrow("select * from NAMES where CUI = '?' "
                             .replace('?', str(self.get_concept_id(cui))))


    def concept_definition(self, cui):
        """
        Get concept definition (if available)
        SAB refers to "source vocabulary"

        Example entry:
        +----------+------------------------------------------------------------------+-----+----------+
        | CUI      | DEF                                                              | SAB | SUPPRESS |
        +----------+------------------------------------------------------------------+-----+----------+
        | C0266139 | Congenital tracheoesophageal fistula without esophageal atresia. | NCI | N        |
        +----------+------------------------------------------------------------------+-----+----------+

        :param cui: medgen concept
        :return: dict(CUI, DEF, SAB)
        """
        return self.fetchrow("select * from MGDEF where CUI = '?' "
                             .replace('?', str(self.get_concept_id(cui))))


    def concept_relations(self, cui):
        """
        Relate concepts.
        Relationships were sources from UMLS, the Unified Medical Language System.

        Each relationship is defined by 'REL' and optionally

        http://www.ncbi.nlm.nih.gov/books/NBK9685/
        http://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/columns_data_elements.html
        http://www.nlm.nih.gov/research/umls/implementation_resources/query_diagrams/er1.html

        Counts of each type:
        select REL,count(*) as cnt from MGREL group by REL order by cnt desc
        +-----+--------+
        | REL | cnt    |
        +-----+--------+
        | RO  | 735308 | Relate "Other"
        | RB  | 138570 | Relate "Broader"
        | RN  | 138570 | Relate "Narrower"
        | SY  | 112964 | Relate "Synonym"
        | SIB |  92934 | Sibling.
        | CHD |  85812 | Child.
        | PAR |  85812 | Parent.
        | AQ  |  21388 |
        | QB  |  21388 |
        | RQ  |  18946 |
        +-----+--------+

        +-----+--------------------------------------------------------+--------+
        | REL | RELA                                                   | cnt    |
        +-----+--------------------------------------------------------+--------+
        | RO  | has_manifestation                                      | 177270 |
        | RO  | manifestation_of                                       | 177270 |
        | RN  | mapped_to                                              |  93163 |
        | RB  | mapped_from                                            |  93163 |
        | SIB |                                                        |  92934 |
        | RO  | disease_has_finding                                    |  49583 |
        | RO  | is_finding_of_disease                                  |  49583 |
        | PAR | inverse_isa                                            |  47643 |
        | CHD | isa                                                    |  47643 |
        | SY  | permuted_term_of                                       |  39654 |
        | SY  | has_permuted_term                                      |  39654 |
        ...


        mysql> desc MGREL;
        +----------+--------------+------+-----+---------+-------+
        | Field    | Type         | Null | Key | Default | Extra |
        +----------+--------------+------+-----+---------+-------+
        | CUI1     | char(8)      | NO   | MUL | NULL    |       |
        | AUI1     | varchar(9)   | YES  |     | NULL    |       |
        | STYPE1   | varchar(50)  | NO   |     | NULL    |       |
        | REL      | varchar(4)   | NO   | MUL | NULL    |       |
        | CUI2     | char(8)      | NO   | MUL | NULL    |       |
        | AUI2     | varchar(9)   | YES  |     | NULL    |       |
        | RELA     | varchar(100) | YES  | MUL | NULL    |       |
        | RUI      | varchar(10)  | NO   |     | NULL    |       |
        | SAB      | varchar(40)  | NO   |     | NULL    |       |
        | SL       | varchar(40)  | NO   |     | NULL    |       |
        | SUPPRESS | char(1)      | NO   |     | NULL    |       |
        +----------+--------------+------+-----+---------+-------+

        :param cui: concept id
        :return: relationships defined in MGREL table
        """
        return self.fetchall("select * from MGREL where CUI1 = '?' or CUI2 = '?' "
                             .replace('?', str(self.get_concept_id(cui))))

    def concept_sources(self, cui):
        """
        Get source vocabulary names (dictionary abbreviations) for a given concept.
        :param cui: MedGen concept
        :return: list of dictionary names (SourceVocab)
        """
        return self.fetchall("select distinct SourceVocab from view_concept where ConceptID = '?' "
                             .replace('?', str(self.get_concept_id(cui))))

    def medgen2umls(self, medgen_uid):
        """
        Get CUI for UID. This is for compatibility purposes.

        :param medgen_uid: int like 651, which points to C0006142
        :return: concept code like "C0006142"
        """
        select_template = '''select ConceptID as ID from view_medgen_uid where MedGenUID = "%s";'''
        return self.fetchID(select_template % medgen_uid)

    def umls2medgen(self, cui):
        """
        Get UID for concept id. This is for compatibility purposes.

        :param cui: concept code like "C0006142"
        :return: int like 651, which points to C0006142
        """
        select_template = '''select MedGenUID as ID from view_medgen_uid where ConceptID = "%s";'''
        return self.fetchID(select_template % cui)

    def get_concept_id(self, unique_id):
        """
        Guard: force usage of MedGen CUI based identifier.

        :param unique_id: either CUI or a UID
        :return: CUI
        """
        if is_format_umls(unique_id):
            return unique_id

        if is_format_medgen(unique_id):
            return self.medgen2umls(unique_id)

        raise Exception('Unknown concept unique identifier format for %s' + unique_id)

    def select_hpo_view_medgen_hpo(self, cui):
        """
        HPO Human Phenotype Ontology
        :param cui: MedGen concept
        :return: MySQL result
        +---------------+--------------+------+-----+---------+-------+
        | Field         | Type         | Null | Key | Default | Extra |
        +---------------+--------------+------+-----+---------+-------+
        | SourceVocab   | varchar(20)  | NO   | MUL | NULL    |       |
        | SemanticType  | varchar(50)  | NO   | MUL | NULL    |       |
        | ConceptID     | char(8)      | NO   | MUL | NULL    |       |
        | ConceptName   | text         | NO   |     | NULL    |       |
        | PhenotypeName | text         | NO   |     | NULL    |       |
        | HPO_ID        | varchar(100) | YES  | MUL | NULL    |       |
        +---------------+--------------+------+-----+---------+-------+
        """
        return self.fetchall(
            "select * from medgen.view_medgen_hpo where ConceptID = '%s' " % cui)

    def select_mim_from_cui(self, cui):
        return self.fetchlist("select DISTINCT MIM_number from medgen_hpo_omim where omim_cui='{}'".format(cui), "MIM_number")






