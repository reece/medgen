from unittest import TestCase
from hamcrest import *
from medgen.annotate.ncbi_variant import ClinvarVariationID, ClinvarPubmeds, NCBIVariantPubmeds, ClinvarAccession, \
    ClinvarAlleleID
from medgen.annotate.pubmed import PMCID2Article
from medgen.db.clinvar import ClinVarDB


class TestClinVarDB(TestCase):
    def setUp(self):
        self.db = ClinVarDB()

    def test_get_random_example_hgvs(self):
        examples = self.db.random_example_hgvs()
        assert_that(len(examples), equal_to(1))
        examples = self.db.random_example_hgvs(10)
        assert_that(len(examples), equal_to(10))
        assert_that(examples[0], starts_with('NM_'))
        print examples

    def test_BIOMED_310_citations_specific_to_clinvar(self):
        pubmeds = ClinvarPubmeds('NM_000410.3:c.845G>A')

        assert_that(len(pubmeds) > 35)
        assert_that(10673304, is_in(pubmeds))
        assert_that(9439654, is_in(pubmeds))

    def test_convert_bidirectionally_hgvs_c_variation_id(self):
        variation_id = 47706
        hgvs_text = 'NM_133378.4:c.98772T>C'

        assert_that(variation_id in ClinvarVariationID(hgvs_text))
        assert_that(variation_id in ClinVarDB().variation_id_for_hgvs_text(hgvs_text))
        assert_that(hgvs_text in ClinVarDB().hgvs_text_for_variation_id(variation_id))

    def test_convert_pubmed_central_PMCID_to_pubmed_PMID(self):
        pmcid = int(3110945)
        pmid = int(15371902)

        assert_that(int(PMCID2Article(pmcid).pmid), equal_to(pmid))

        hgvs_text = 'NM_000492.3:c.1519_1521delATC'

        assert_that(pmid in ClinvarPubmeds(hgvs_text))
        assert_that(pmcid not in ClinvarPubmeds(hgvs_text))

    # NKF - 8/11/15 - commented out test due to NCBI Variant Reporter being down
    def _test_well_cited_variant_has_some_different_clinvar_dbsnp_entries(self):
        hgvs_text = 'NM_000492.3:c.1521_1523delCTT'

        ncbi_pmids = NCBIVariantPubmeds(hgvs_text)
        ncbi_pmids = set([int(entry) for entry in ncbi_pmids])

        clinvar_pmids = ClinvarPubmeds(hgvs_text)

        # This paper is cited by dbSNP but not ClinVar
        #
        # Proof:
        #      http://www.ncbi.nlm.nih.gov/snp?LinkName=pubmed_snp_cited&from_uid=15948195
        #
        assert_that(15948195 in ncbi_pmids)
        assert_that(15948195 not in clinvar_pmids)

        # ClinVar knows about this PMID but Variation Reporter does not.
        # Probably because it is cited by PubMedCentral and not Pubmed.
        #
        assert_that(15371902 in clinvar_pmids)
        assert_that(15371902 not in ncbi_pmids)
        assert_that(15371902, equal_to(int(PMCID2Article(3110945).pmid)))

    def test_BIOMED_413_accession_and_variation_id_for_hgvs_c(self):
        hgvs_c = 'NM_001232.3:c.919G>C'
        allele_id = 32649
        variation_id = 17610
        accession = 'RCV000019176'

        assert_that(accession in ClinVarDB().accession_for_hgvs_text(hgvs_c))
        assert_that(variation_id in ClinVarDB().variation_id_for_hgvs_text(hgvs_c))
        assert_that(allele_id in ClinVarDB().allele_id_for_hgvs_text(hgvs_c))
        assert_that(hgvs_c in ClinVarDB().hgvs_text_for_variation_id(variation_id))

    def test_BIOMED_405_hgvs_c_was_null_slang_used_in_variant_name(self):
        hgvs_c = 'NM_002485.4:c.1222A>G'
        variation_id = 127856
        allele_id = 133313
        accession1 = 'RCV000115778'
        accession2 = 'RCV000119194'

        assert_that(accession1 in ClinVarDB().accession_for_hgvs_text(hgvs_c))
        assert_that(accession2 in ClinVarDB().accession_for_hgvs_text(hgvs_c))
        assert_that(variation_id in ClinVarDB().variation_id_for_hgvs_text(hgvs_c))
        assert_that(allele_id in ClinVarDB().allele_id_for_hgvs_text(hgvs_c))
        assert_that(hgvs_c in ClinVarDB().hgvs_text_for_variation_id(variation_id))

    def test_BIOMED_405_find_hgvs_for_previously_entered_LRG_slang_sloppy_variant_name(self):
        hgvs_c = 'NM_004006.2:c.5586+18A>G'
        variation_id = 94672
        allele_id = 100572
        accession = 'RCV000080662'

        assert_that(accession in ClinVarDB().accession_for_hgvs_text(hgvs_c))
        assert_that(variation_id in ClinVarDB().variation_id_for_hgvs_text(hgvs_c))
        assert_that(allele_id in ClinVarDB().allele_id_for_hgvs_text(hgvs_c))
        assert_that(hgvs_c in ClinVarDB().hgvs_text_for_variation_id(variation_id))

    def test_BIOMED_405__has_accession_for_previously_null_hgvs_c(self):
        hgvs_text = 'NM_000016.4:c.1091T>C'
        accession = 'RCV000077876'
        variation_id = 92253
        pmid = 23757202

        assert_that(accession in ClinvarAccession(hgvs_text))
        assert_that(variation_id in ClinvarVariationID(hgvs_text))

        assert_that(pmid in ClinvarPubmeds(hgvs_text))

    def test_BIOMED_405__new_hgvs_c_entry_for_previously_null_hgvs_c(self):
        for hgvs_text in [
            # 'NM_138370.1:c.168C>T',
            'NM_058216.1:c.706-2A>G'
            , 'NM_001099404.1:c.3556G>A'
            #, 'NM_001033756.2:c.963-1174C>T'
            , 'NM_181798.1:c.785C>A'
            , 'NM_000368.3:c.1579C>T'
            #, 'NM_001164883.1:c.2298+14940A>G'
            #, 'NM_152404.3:c.577G>A'
            , 'NM_000314.4:c.170T>G'
            , 'NM_198056.2:c.3907C>A'
            , 'NM_023035.2:c.4375G>T'
            , 'NM_000257.2:c.2683C>G'
            , 'NM_000060.2:c.1227_1241delins11'
            #, 'NM_001130455.1:c.564G>A'
            #, 'NM_003815.4:c.840C>T'
            , 'NM_001267550.1:c.67571G>A'
            , 'NM_000057.2:c.1544dupA'
            , 'NM_007294.3:c.2679_2682del'
            #, 'NM_020142.3:c.-264-u3504C>T'
        ]:
            print hgvs_text
            assert_that(len(ClinvarVariationID(hgvs_text)) >= 1)
            assert_that(len(ClinvarAccession(hgvs_text)) >= 1)

    # NKF - 8/11/15 - commented out test due to NCBI Variant Reporter being down
    def _test_BIOMED_405__haplotype_has_multiple_variation_id_for_single_hgvs(self):
        hgvs_text = 'NM_000060.2:c.133G>A'

        pmid_expected = NCBIVariantPubmeds(hgvs_text)
        pmid_expected = set([int(id) for id in pmid_expected])

        assert_that(3 == len(ClinvarVariationID(hgvs_text)))
        assert_that(1 == len(ClinvarAlleleID(hgvs_text)))
        assert_that(3 == len(ClinvarPubmeds(hgvs_text)))

        assert_that(set(ClinvarPubmeds(hgvs_text)) == pmid_expected)

    def test_get_version(self):
        version_str = self.db.get_version()
        assert_that(len(version_str), greater_than(0))
