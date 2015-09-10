from unittest import TestCase
from hamcrest import *
from datetime import datetime
from medgen.db.dataset import SQLData

class TestSQLData(TestCase):

    def test_non_existing_db(self):
        db = SQLData(dataset='ThisDBDoesNotExist')
        assert_that(calling(db.fetchall).with_args("select * from my_table"), raises(Exception))
        assert_that(calling(db.fetchrow).with_args("select * from my_table"), raises(Exception))
        assert_that(calling(db.fetchID).with_args("select * from my_table"), raises(Exception))

    def test_non_existing_table(self):
        db = SQLData(config_section='clinvar')
        assert_that(calling(db.fetchall).with_args("select * from my_table"), raises(Exception))
        assert_that(calling(db.fetchrow).with_args("select * from my_table"), raises(Exception))
        assert_that(calling(db.fetchID).with_args("select * from my_table"), raises(Exception))

    def test_fetch_with_no_results(self):
        db = SQLData(config_section='clinvar')

        sql_query ='select distinct HGVS_c from variant_summary where AlleleID = "Not-An-Allele-ID"'
        assert_that(len(db.fetchall(sql_query)), is_(0))
        assert_that(db.fetchrow(sql_query), is_(None))
        assert_that(db.fetchID(sql_query), is_(None))

        sql_query_with_ID ='select distinct HGVS_c as ID from variant_summary where AlleleID = "15041"'
        assert_that(db.fetchID(sql_query_with_ID), is_('NM_014855.2:c.80_83delGGATinsTGCTGTAAACTGTAACTGTAAA'))

        sql_query_with_missing_ID ='select distinct HGVS_c from variant_summary where AlleleID = "15041"'
        assert_that(calling(db.fetchID).with_args(sql_query_with_missing_ID), raises(Exception))

    def test_get_last_mirror_time(self):
        db = SQLData(config_section='clinvar')
        last_mirror_time = db.get_last_mirror_time("variant_summary")
        assert_that(last_mirror_time, greater_than_or_equal_to(datetime(2015, 1, 1)))
        assert_that(calling(db.get_last_mirror_time).with_args("table_that_does_not_exist"), raises(Exception))
















