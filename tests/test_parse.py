import unittest
from hamcrest import equal_to, assert_that, is_, greater_than, is_in, has_entries, has_key

from medgen.parse.gene    import Gene
from medgen.parse.concept import Concept

class ParseTestCase(unittest.TestCase):

    def test_gene(self):
        assert_that(Gene('BRCA2'),      equal_to(Gene('BRCA2')))
        assert_that(Gene('BRCA2').name, equal_to('BRCA2'))
        assert_that(Gene('BRCA2').id,   equal_to(675))

    def test_concept(self):
        assert_that(Concept('C0007194'), equal_to(Concept('C0007194')))
        assert_that(Concept('2881'),     equal_to(Concept('2881')))
        assert_that(Concept('2881'),     equal_to(Concept(2881)))
        assert_that(Concept(2881).umls_cui, equal_to(Concept('C0007194').umls_cui))
        #assert_that(Concept('C0007194').medgen_uid, equal_to(Concept(2881).medgen_uid))
        #self.assertEqual(Concept('C0007194').medgen_uid, Concept(2881).medgen_uid)

if __name__ == '__main__':
    unittest.main()