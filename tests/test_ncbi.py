from unittest import TestCase
from hamcrest import assert_that, contains_inanyorder

from medgen.annotate.ncbi_variant import NCBIVariantPubmeds

class NcbiVariant_TestCase(TestCase):

    # NKF - 8/11/15 - commented out test due to NCBI Variant Reporter being down
    def _test_biomed_307(self):
        '''
        :return: If there are no PMIDs, the result is empyt string
        '''
        hgvs = 'NM_020975.4:c.1702G>A'
        result = NCBIVariantPubmeds(hgvs)
        assert_that(not result)  # result is empty string

        hgvs = 'NM_001232.3:c.919G>C'
        result = NCBIVariantPubmeds(hgvs)
        assert_that(result, contains_inanyorder(11704930, 16908766, 20301466))
