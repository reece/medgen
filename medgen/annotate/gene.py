# from __future__ import absolute_import
##########################################################################################
from collections import OrderedDict

from ..log        import log
from ..db.gene    import GeneDB
from ..db.hugo    import HugoDB
from ..db.clinvar import ClinVarDB

##########################################################################################
#
#       Functions
#
##########################################################################################

def _gene_locus_databases(gene):
    """
    Get Locus Specific Databases for Gene
    :param gene: hugo gene name
    :return: array of LSDBs from Hugo Gene, the official source of gene names.
    """
    try:
        row = HugoDB().get_locus_specific_databases(gene)

        parsed = OrderedDict()
        parsed['Gene'] = str(gene)

        dbs  = OrderedDict()
        text = row['LocusSpecificDatabases']

        if text is not None and len(text) > 1:
            for csv in text.split(','):
                (name, url) = csv.split('|')
                dbs[name] = url

        parsed['LocusSpecificDatabases'] = dbs

        pubmds = []
        text = row['pubmeds']

        if text is not None and len(text) > 0:
            for pmid in text.split(','):
                pmid = pmid.strip(' ')
                pubmds.append(str(pmid)) # @TODO: MetaPub?
                #pubmds.append(PubMed(pmid)) # @TODO: MetaPub?

            parsed['pubmeds'] = pubmds

        return parsed
    except Exception as e:
        msg = 'Failed to fetch LocusSpecificDatabases for gene_symbol'
        log.error(msg)
        raise e

def _gene_synonyms(gene):
    """
    Each gene can have multiple GeneSynonyms -- names that refer to the same gene.
    :param gene: string hugo gene name (hgnc)
    :return: set of gene symbols and names, including unofficial ones.
    """
    synonyms = set()

    if gene is None:
       log.warn('Could not get HGNC GeneName synonyms' )
       return None
    else:
       aliases = GeneDB().get_gene_synonyms(gene)

       if (aliases is None) or (len(aliases) < 1):
           raise Exception('could not retrieve gene SYNONYMS for '+gene)
       else:
           for row in aliases:
               synonyms.add(row.get('Symbol'))

               for gene_alias in row.get('Synonyms').split('|'):
                   synonyms.add(gene_alias)

    return sorted(synonyms)

def _gene_preferred(gene):
    """
     Each gene can have multiple GeneSynonyms.
     This function returns the one preferred by HUGO.
    :param gene: string gene symbol
    :return: string of preferred symbol
    """
    prefered = set()

    if gene is None:
       return None
    else:
       aliases = GeneDB().get_gene_synonyms(gene)

       if (aliases is None) or (len(aliases) < 1):
           raise Exception('could not retrieve PREFERRED gene name for '+gene)
       else:
           for row in aliases:
               prefered.add(row.get('Symbol'))

    return sorted(prefered)

##########################################################################################
#
#       API
#
##########################################################################################

Gene2PubMed      = GeneDB().gene2pubmed
Gene2Function    = GeneDB().gene_function
Gene2LocusDB     = _gene_locus_databases

Gene2MIM                  = GeneDB().gene2mim
Gene2ConditionSource      = ClinVarDB().gene2condition
Gene2ClinicalSignificance = ClinVarDB().gene_to_clinical_significance_type_frequency

GeneInfo          = GeneDB().get_gene_info
GeneID            = GeneDB().get_gene_id
GeneName          = GeneDB().get_gene_name
GeneSynonyms      = _gene_synonyms
GeneNamePreferred = _gene_preferred


