from __future__ import absolute_import
import re
import datetime
import MySQLdb
from metapub import PubMedArticle
from metapub.utils import asciify
from .dataset import SQLData

##########################################################################################
#
#       SQLData Class
#
##########################################################################################


class PubMedDB(SQLData):
    """
    Common access methods for PubMed tables.
    See also https://pypi.python.org/pypi/metapub
    """
    def __init__(self):
        super(PubMedDB, self).__init__(config_section='pubmed')


    def abstract_text(self, pmid):
        """
        Get the abstract for a pmid
        :var pmid : pmid
        :returns abstract_text
        """
        sql = """
            select abstract_text
            from   medline_minimum_citation
            where  PMID = '@PMID'
            """. \
            replace("@PMID", str(pmid))
        return self.fetchrow(sql)

    def medline_xml_filename_insert(self, filename):
        """
        Book Keeping method for Medline XML
        :param filename: name of medline_xml file
        :return: result of SQL insert
        """
        return self.insert("medline_xml_filename", {"filename" : filename})

    def _get_tstamp(self, xml_block):
        """
        <DateCreated>
        <Year>2001</Year>
        <Month>07</Month>
        <Day>16</Day>
        </DateCreated>

        :return datetime for day
        """
        y_match = re.search(r"<Year>(\d+)</Year>", xml_block)
        m_match  = re.search(r"<Month>(\d+)</Month>", xml_block)
        d_match  = re.search(r"<Day>(\d+)</Day>", xml_block)
        if y_match and m_match and d_match:
            y = int(y_match.group(1))
            m = int(m_match.group(1))
            d = int(d_match.group(1))
            return datetime.date(year=y, month=m, day=d)
        return None

    def _get_last_date(self, medline_citation_xml_text):
        """
        Get the last revision/completed/or created date:
        <DateCreated>
        <Year>2001</Year>
        <Month>06</Month>
        <Day>21</Day>
        </DateCreated>
        <DateCompleted>
        <Year>2001</Year>
        <Month>07</Month>
        <Day>05</Day>
        </DateCompleted>
        <DateRevised>
        <Year>2010</Year>
        <Month>11</Month>
        <Day>18</Day>
        </DateRevised>
        :param medline_citation_xml_text: "<MedlineCitation>...</MedlineCitation>" xml text
        :return (year, month, date) as integer strings, like ('2013', '09', '15')
        """
        match_revised = re.search(r"<DateRevised>.+</DateRevised>", medline_citation_xml_text, re.DOTALL)
        if match_revised:
            return self._get_tstamp(match_revised.group(0))

        match_completed = re.search(r"<DateCompleted>.+</DateCompleted>", medline_citation_xml_text, re.DOTALL)
        if match_completed:
            return self._get_tstamp(match_completed.group(0))

        match_created = re.search(r"<DateCreated>.+</DateCreated>", medline_citation_xml_text, re.DOTALL)
        if match_created:
            return self._get_tstamp(match_created.group(0))

        raise RuntimeError("Could not find date in %s" % medline_citation_xml_text)

    def _get_PMID(self, medline_citation_xml_text):
        """
        Parse out the PMID from medline XML
        :param medline_citation_xml_text: string containing medline xml
        :return: PMID as a str
        """
        match = re.search(r'<PMID Version="\d+">(\d+)</PMID>', medline_citation_xml_text)
        return match.group(1)

    def medline_xml_select_ids(self, min_id=0):
        """
        Get all IDs in the medline_xml table thare are
        greater than min_id
        :param min_id: the minimum id
        :return: list of ids in ascending order
        """
        sql_query = "SELECT id from medline_xml where id>%d order by id" % min_id
        rows = self.fetchall(sql_query)
        return [row['id'] for row in rows]

    def medline_xml_select_by_pmid(self, pmid):
        """
        fetch row with PMID
        :param pmid:
        :return: row (dict)
        """
        sql_query = "SELECT * FROM medline_xml WHERE pmid=%s" % str(pmid)
        return self.fetchrow(sql_query)

    def medline_xml_select_by_pmid_and_max_tstamp(self, pmid, max_tstamp):
        """
        fetch row with PMID and max Tstamp
        :param pmid (int or str)
        :param max_tstamp (date)
        :return: row (dict)
        """
        sql_query = 'SELECT * FROM medline_xml WHERE pmid=%s and Tstamp<"%s"' % (str(pmid), str(max_tstamp))
        return self.fetchrow(sql_query)


    def medline_xml_select_by_id(self, id):
        """
        fetch row with ID
        :param id: ID of row
        :return: row (dict)
        """
        sql_query = "SELECT * FROM medline_xml WHERE id=%s" % str(id)
        return self.fetchrow(sql_query)

    def medline_xml_update(self, xml, filename_id):
        """
        Insert or update the parsed HGVS variant record

        Will only update if the xml citation has a newer Tstamp
        than the one in the DB

        To commit, must run:
        PySQLPool.commitPool()

        :param xml: xml (str)
        :return: result of sql query for insertion or update, or None of nothing was inserted
        """
        pmid = self._get_PMID(xml)
        last_date = self._get_last_date(xml)

        # find any row with the same PMID and that are older than max stamp
        record_in_db = self.medline_xml_select_by_pmid_and_max_tstamp(pmid, last_date)

        # update if there is no xml for the PMID, or the xml is newer than the one in the DB
        if record_in_db is None:
            d = {"PMID" : pmid, "xml" : xml, "Tstamp" : str(last_date), "medline_xml_filename_id" : filename_id }
            return self.insert("medline_xml", d)

            # sql_values = (
            #     str(pmid),
            #     MySQLdb.escape_string(xml),
            #     str(last_date),
            #     str(filename_id)
            # )
            # sql_insert = 'INSERT INTO medline_xml (PMID, xml, Tstamp, medline_xml_filename_id) VALUES (%s, "%s", "%s", %s) ' % sql_values
            # sql_insert += 'ON DUPLICATE KEY UPDATE xml=values(xml), Tstamp=values(Tstamp), medline_xml_filename_id=values(medline_xml_filename_id)'
            #
            # return self.execute(sql_insert)

        return None


