import re
from collections import OrderedDict

##########################################################################################
#
#       Functions
#
##########################################################################################

def split_quoted(text):
  matches=re.findall(r'\"(.+?)\"',text)
  return ",".join(matches)


def split_csv_tsv(text):
    res = OrderedDict()

    for csv in text.split(','):
        for tsv in csv.split('|'):
            res.items().append(tsv)

    return res




