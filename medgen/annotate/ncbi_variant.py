# from __future__ import absolute_import

import requests, json, urllib
from ..db.clinvar import ClinVarDB
from ..log import log, IS_DEBUG_ENABLED

##########################################################################################
#
#   Functions
#
##########################################################################################
from medgen.annotate.pubmed import PMCID2Article


def _ncbi_variant_report_service(hgvs_text):
    """
    Return results from API query to the NCBI Variant Reporter Service
    See documentation at:
    http://www.ncbi.nlm.nih.gov/variation/tools/reporter
    :param hgvs_text: ( c.DNA | r.RNA | p.Protein | g.Genomic )
    :return: JSON (dictionary)
    """
    #r = requests.post("http://www.ncbi.nlm.nih.gov/projects/SNP/VariantAnalyzer/var_rep.cgi", data={"annot1": hgvs_text})
    hgvs_text = str(hgvs_text)
    r = requests.get("http://www.ncbi.nlm.nih.gov/projects/SNP/VariantAnalyzer/var_rep.cgi?annot1={}".format(urllib.quote(hgvs_text)))
    res = r.text

    if 'Error' in res:
        error_str = 'An error occurred when using the NCBI Variant Report Service: "{}"\n'.format(res)
        error_str += 'To reproduce, visit: http://www.ncbi.nlm.nih.gov/projects/SNP/VariantAnalyzer/var_rep.cgi?annot1={}'.format(hgvs_text)
        raise RuntimeError(error_str)

    if IS_DEBUG_ENABLED:
        log.debug(res)

    res = res.split('\n')
    res = filter(
        lambda x: x != '' and not str.startswith(str(x), '.') and not str.startswith(str(x), '##') and not str.startswith(str(x), "Submitted"),
        res)
    res = map(lambda x: x.split('\t'), res)
    keys = map(lambda x: x.strip('# '), res[0])
    values = res[1:]
    res = map(lambda x: dict(zip(keys, x)), values)
    for r in res:
        if r.has_key('PMIDs'):
            if len(r['PMIDs']) == 0:
                r['PMIDs'] = []
            else:
                r['PMIDs'] = r.get('PMIDs').replace(', ', ';').split(';')

    return res


def _ncbi_variant_pubmeds(hgvs_text=None):
    """
    Retrieve PMIDs for a variant from the NCBI Variant Reporter Service
    :param hgvs_text:  ( c.DNA | r.RNA | p.Protein | g.Genomic )
    :return: list(PMID)
    """
    _report  = _ncbi_variant_report_service(hgvs_text)
    _pubmeds = None

    for _row in _report:
        _pubmeds = _row['PMIDs']
        if _pubmeds is not None:
            for _pmid in _pubmeds:
                if len(str(_pmid)) > 1:
                    pass

    return map(int, _pubmeds)

def _service_report_accession(hgvs_text=None):
    """
    Retrieve accession for a variant from the NCBI Variant Reporter Service
    :param hgvs_text: ( c.DNA | r.RNA | p.Protein | g.Genomic )
    :return: Accession (clinvar | dbSNP | dbVar)
    """
    _report  = _ncbi_variant_report_service(hgvs_text)

    for _row in _report:
        _accession = _row['ClinVar Accession']

        if _accession is not None:
            if len(str(_accession)) > 1:
                    return _accession

    log.debug("Variant accession not found in clinvar for "+ str(hgvs_text))
    return None

def _clinvar_variant_accession(hgvs_text):
    """
    See ClinVar FAQ http://www.ncbi.nlm.nih.gov/clinvar/docs/faq/#accs
    :param hgvs_text: c.DNA
    :return: RCVAccession "Reference ClinVar Accession"
    """
    try:
        return ClinVarDB().accession_for_hgvs_text(str(hgvs_text))
    except Exception, e:
        log.debug("no clinvar accession for variant hgvs_text %s " % hgvs_text)

def _clinvar_variant_allele_id(hgvs_text):
    """
    Get the unique AlleleID
    :param hgvs_text: c.DNA
    :return: AlleleID
    """
    try:
        return ClinVarDB().allele_id_for_hgvs_text(hgvs_text)
    except Exception, e:
        log.debug('no clinvar AlleleID for variant hgvs_text %s ' % hgvs_text)

def _clinvar_variant_variation_id(hgvs_text):
    """
    Get the unique VariationID
    :param hgvs_text: c.DNA
    :return: VariationID
    """
    try:
        return ClinVarDB().variation_id_for_hgvs_text(hgvs_text)
    except Exception, e:
        log.debug('no clinvar VariationID for variant hgvs_text %s ' % hgvs_text)

def _clinvar_variant2pubmed(hgvs_text):
    """
    Get PMID for clinvar variants using the AlleleID key.
    If the citation_source is PubMedCentral, first convert responses to PMID.
    :param hgvs_text: c.DNA
    :return: list(PMID)
    """
    pubmeds = []
    citations = ClinVarDB().var_citations(hgvs_text)
    if citations:
        for cite in citations:
            pmid = cite['citation_id']

            if cite['citation_source'] == 'PubMed':
                pubmeds.append(pmid)
            else:
                log.debug('found PubMedCentral PMCID, converting to PMID %s ' % str(pmid))
                article = PMCID2Article(pmid)

                if article is not None:
                    pubmeds.append(int(article.pmid))

    return set([int(entry) for entry in pubmeds])


def clinvar2pmid_with_accessions(hgvs_list):
    ret = []
    citations = ClinVarDB().var_citations(hgvs_list)
    if citations:
        for cite in citations:
            article_id = cite['citation_id']
            pmid = article_id if cite['citation_source'] == 'PubMed' else PMCID2Article(article_id).pmid
            if pmid:
                ret.append({"hgvs_text": cite['hgvs_text'], "pmid": pmid, "accession": cite['RCVaccession']})
    return ret


##########################################################################################
#
# API
#
##########################################################################################

NCBIVariantReport  = _ncbi_variant_report_service
NCBIVariantPubmeds = _ncbi_variant_pubmeds
ClinvarAccession   = _clinvar_variant_accession
ClinvarAlleleID    = _clinvar_variant_allele_id
ClinvarPubmeds     = _clinvar_variant2pubmed
ClinvarVariationID = _clinvar_variant_variation_id
