#### common
# from __future__ import absolute_import
from collections import OrderedDict

#### metapub
from metapub import PubMedFetcher, PubMedArticle
from medgen.db.medgen import MedGenDB

##########################################################################################
#
#  Functions
#
##########################################################################################

def _pubmed_pmid_to_article(pmid):
    """
     Use eutils to fetch pubmed article information.
     TODO: integration with eutils.
    :param pmid: int or str
    :return: PubMedArticle
    """
    return PubMedFetcher('eutils').article_by_pmid(str(pmid))

def _pubmed_central_pmcid_to_article(pmcid):
    """
    Specific to PMC PubMed Central.
     Use eutils to fetch pubmed article information.
     TODO: integration with eutils.
    :param pmcid:
    :return: PubMedArticle
    """
    return PubMedFetcher('eutils').article_by_pmcid(str(pmcid))

##########################################################################################
#
#       API
#
##########################################################################################
PMID2Article = _pubmed_pmid_to_article
PMCID2Article = _pubmed_central_pmcid_to_article