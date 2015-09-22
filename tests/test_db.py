from unittest import TestCase
from hamcrest import assert_that, is_
########################################
from medgen.db.dataset import SQLData
from medgen.db.pubmed  import PubMedDB
from medgen.db.clinvar import ClinVarDB
from medgen.db.gene    import GeneDB
from medgen.db.medgen  import MedGenDB
from medgen.db.hugo    import HugoDB
from medgen.db.personalgenomes import PersonalGenomesDB
########################################

class ConnectDBsTestCase(TestCase):
    DB_LIST = [GeneDB, HugoDB, MedGenDB, PubMedDB, ClinVarDB]

    def test_connections(self):
        for d in self.DB_LIST:
            db = d()
            ping = db.ping()
            assert_that(ping['tables'][0]['table_schema'], is_(db._db_name))

    def test_truncate_str(self):
        s = "NP_775931.3:p.(Pro504delinsArgGluProGlnIleProProArgGlyCysLysGlyAlaGluPheAlaProArgTrpGlnArgLysTrpArgGlnProProCysArgLeuValLeuCysValLeuTrpGluGlyProGlyValSerArgArgGlyGluLeuGluGlyAlaProCysGlyCysHisArgArgLysGlyLeuThrTrpGlyGlyGluPheTrpLysAlaGlyAlaLeuGlyProAlaGlyArgGlyHisGlnSerProAsnAlaGlnLeuLeuHisSerValSerProThrProGluAspGlnValSerAlaAlaProLeuLeuAlaArgAlaLeuHisTrpGlyAlaLysGlyTrpArgProCysArgTrpProCysProProTrpAlaSerArgProLeuArgGlyTrpProValLeuProIleThrSerLeuGlyGlnSerHisHisLeuLeuSerIleLysLeuProGlnArgLeuArgProProGlyLeuHisGlnProSerProProGlyLeuArgValArgTrpAlaSerSerProSerMetGlyGlyAsn)"
        db = SQLData(dataset='medgen')
        s_truncated = db.trunc_str(s, 200)
        assert_that(len(s_truncated), is_(200))




