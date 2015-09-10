#### test
from unittest import TestCase
from hamcrest import assert_that, is_

#### medgen
from medgen.api import Gene, GeneDB, Gene2PubMed, Gene2LocusDB, Gene2Function, GeneID, GeneNamePreferred, GeneName

class GeneTestCase(TestCase):
    gene_id = 675
    gene_name = 'BRCA2'

    def test_id(self):
        gene = Gene(self.gene_id)
        assert_that(GeneID(self.gene_id), is_(self.gene_id))
        assert_that(GeneName(self.gene_id), is_(self.gene_name))
        assert_that(gene.id, is_(self.gene_id))
        assert_that(gene.name, is_(self.gene_name))

    def test_api(self):
        Gene(self.gene_id)
        Gene2PubMed(self.gene_id)
        Gene2LocusDB(self.gene_id)
        Gene2Function(self.gene_id)