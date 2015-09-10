=================
medgen
=================
what is medgen? 
===================
**medgen** provides functions for annotating **Medical Genetics** information: genes and conditions from the NCBI MedGen databases (https://bitbucket.org/invitae/medgen-mysql). For example, the **Gene2Condition** service uses annotations from this database for annotating diseases associated with a specific gene, including the **mode of inheritance** and extensive phenotypic descriptions from **MedGen** linked data sources including OMIM and HPO (Human Phenotype Ontology). Use of MedGen extends from variant level details provided by **ClinVar** all the way to disease descriptions used in hospital medical record systems (**SNOMED**) and billing systems (**ICD9**). 

Depends
----------------
medgen-python uses either a local or hosted set of https://bitbucket.org/invitae/medgen-mysql databases.
See **requirements.txt** for the list of python required packages. 


support and licensing
=====================

medgen python is a free and open source library provided by Invitae under the [Apache 2.0 License](http://www.apache.org/licenses/), a copy of which is included within the repository.

All questions, concerns, support, and curse words should be directed to package maintainers
Andrew McMurry (AndyMC@apache.org) and BioMed contributors ( BioMed@invitae.com ).

Contributions to this library are encouraged via fork and pull request. Diffs may be accepted
when attached to nicely written emails.


api-shell 
==============================
::
   
   virtualenv ve 
   source ve/bin/activate
   pip install -r bin/requirements.txt      
   ./api-shell
   whos   
   
   
API Functional documentation
==============================
::
   
   type "whos" to see a list of supported API functions.
   type "function?" to see the docstring for a given function.
   

API (frequency used, not extensive list)
=====================================================

* ClinvarPubmeds?
* ClinvarAccession?
* Concept?
* Define?
* Relate?
* DiseaseName?
* DiseaseParents?
* DiseaseSubtypes?
* Gene?
* Gene2PubMed?
* GeneID?
* GeneInfo?
* GeneNamePreferred?
* GeneSynonyms?
* Gene2ConditionSource?
* Gene2Function?
* Gene2LocusDB?
* GeneName?
* GeneNamePreferred?   
* Gene2PubMed?
* GeneSynonyms?
* MedGenDB?
* NCBIVariantReport?
* NCBIVariantPubmeds?
* PMID2Article?
* PMCID2Article?
* SQLData?

  
SQLData classes
=====================================================

SQLData?
MedGenDB?
ClinVarDB?
GeneDB?
HugoDB?
PubMedDB?


