# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

from pyrfc3339 import parse

import MySQLdb as mdb
import MySQLdb.cursors as cursors
import PySQLPool

from ..log import log, IS_DEBUG_ENABLED

DEFAULT_HOST = 'localhost'
DEFAULT_USER = 'medgen'
DEFAULT_PASS = 'medgen'
DEFAULT_DATASET = 'medgen'

SQLDATE_FMT = '%Y-%m-%d %H:%M:%S'
def EscapeString(value):
    value = value.replace('"', '\\"')
    return '"{}"'.format(value)

def SQLdatetime(pydatetime_or_string):
    if hasattr(pydatetime_or_string, 'strftime'):
        dtobj = pydatetime_or_string
    else:
        # assume pyrfc3339 string
        dtobj = parse(pydatetime_or_string)
    return dtobj.strftime(SQLDATE_FMT)

class SQLData(object):
    """
    MySQL base class for config, select, insert, update, and delete of medgen linked databases.
     TODO: more documentation on config.
    """
    def __init__(self, *args, **kwargs):
        self._cfg_section = kwargs.get('config_section', 'DEFAULT')

        from ..config import config
        self._db_host = kwargs.get('db_host', None) or config.get(self._cfg_section, 'db_host')
        self._db_user = kwargs.get('db_user', None) or config.get(self._cfg_section, 'db_user')
        self._db_pass = kwargs.get('db_pass', None) or config.get(self._cfg_section, 'db_pass')
        self._db_name = kwargs.get('dataset', None) or config.get(self._cfg_section, 'dataset')
        self.commitOnEnd = kwargs.get('commitOnEnd', True) or config.get(self._cfg_section, 'commitOnEnd')

    def connect(self):
        return PySQLPool.getNewConnection(username=self._db_user, 
                password=self._db_pass, host=self._db_host, db=self._db_name, charset='utf8', commitOnEnd=self.commitOnEnd)

    def cursor(self, execute_sql=None):

        conn = self.connect()
        cursor = conn.cursor(cursors.DictCursor)

        if execute_sql is not None:
            cursor.execute(execute_sql)

        return [conn, cursor]

    def commitPool(self):
        '''
        Actively commit all transactions in the entire PySQLPool
        '''
        PySQLPool.commitPool()

    def fetchall(self, select_sql):
        log.debug(select_sql)
        return self.execute(select_sql).record

    def fetchrow(self, select_sql):
        '''
        If the query was successful:
            if 1 or more rows was returned, returns the first one
            else returns None
        Else:
            raises Exception
        '''
        results = self.fetchall(select_sql)
        return results[0] if len(results)>0 else None

    def fetchID(self, select_sql, id_colname='ID'):
        results = self.fetchrow(select_sql)
        if results is not None:
            if id_colname in results:
                return results[id_colname]
            else:
                raise RuntimeError("No ID column found.  SQL query: %s" % select_sql)
        return None  # no results found

    #UNUSED: confirmed not used anywhere in medgen-python or variant2pubmed
    #def fetchall_where(self, select_sql, _value, _key=SQLValues.tic('?')):
    #    return self.fetchall(select_sql.replace(_key, _value))

    #UNUSED: confirmed not used anywhere in medgen-python or variant2pubmed
    #def results2set(self, select_sql, col='PMID'):
    #    pubmeds = set()
    #    for row in self.fetchall(select_sql):
    #        pubmeds.add(str(row[col]))
    #    return pubmeds

    #TODO: (@nthmost) variant2pubmed usage rectification
    #
    # Previous declaration:
    #def insert(self, sql_insert, sql_values, do_tic=True):
    #    """
    #    :param sql_insert: string statement like 'insert into hgvs_query(hgvs_text, .....)'
    #    :param sql_values: list of values
    #    """
    #    if do_tic:
    #       # reconstructed, probably wrong. doesn't matter, we're not using it.
    #       values = SQLValues(sql_values)
    #
    #    return self.execute(sql_insert + " values " + SQLValues.AND(sql_values))

    def insert(self, tablename, field_value_dict):
        '''
        :param: tablename: name of table to receive new row
        :param: field_value_dict: map of field=value
        :return: row_id (integer) (returns 0 if insert failed)
        '''
        fields = []
        values = []

        for k,v in field_value_dict.items():
            if v==None:
                continue
            fields.append(k)
            # surround strings and datetimes with quotation marks
            if hasattr(v, 'strftime'):
                v = '"%s"' % v.strftime(SQLDATE_FMT)
            elif hasattr(v, 'lower'):
                v = EscapeString(v) # surrounds strings with quotes and unicodes them.
            else:
                v = unicode(v)

            values.append(v)

        sql = 'insert into {} ({}) values ({});'.format(tablename, ','.join(fields), ','.join(values))
        #print(sql)
        queryobj = self.execute(sql)
        # retrieve and return the row id of the insert. returns 0 if insert failed.
        return queryobj.lastInsertID

    def update(self, tablename, id_col_name, row_id, field_value_dict):
        '''
        :param: tablename: name of table to update
        :param: row_id (int): row id of record to update
        :param: field_value_dict: map of field=value
        :return: row_id (integer) (returns 0 if insert failed)
        '''
        fields = []
        values = []

        clauses = []

        for k,v in field_value_dict.items():
            clause = '%s=' % k
            # surround strings and datetimes with quotation marks
            if v==None:
                clause += 'NULL'
            elif hasattr(v, 'strftime'):
                clause += '"%s"' % v.strftime(SQLDATE_FMT)
            elif hasattr(v, 'lower'):
                clause += EscapeString(v) #surrounds strings with quotes and unicodes them.
            else:
                clause += unicode(v)
            clauses.append(clause)

        sql = 'update %s set %s where %s=%i;' % (tablename, ', '.join(clauses), id_col_name, row_id)
        queryobj = self.execute(sql)
        # retrieve and return the row id of the insert. returns 0 if insert failed.
        return queryobj.lastInsertID

    def drop_table(self, tablename):
        return self.execute(" drop table if exists " + tablename)

    def truncate(self, tablename):
        return self.execute(" truncate " + tablename)

    def execute(self, sql):
        '''
        Excutes arbitrary sql string in current database connection.    
        Returns results as PySQLPool query object.
        '''
        log.debug('SQL.execute ' + sql)
        queryobj = PySQLPool.getNewQuery(self.connect())
        queryobj.Query(sql)

        return queryobj

    def ping(self):
        '''
        Same effect as calling 'mysql> call mem'
        :returns::self.schema_info(()
        '''
        try:
            return self.schema_info()
        except mdb.Error, e:
            log.error("DB connection is dead %d: %s" % (e.args[0], e.args[1]))
            return False

    def schema_info(self):
        header = ['schema', 'engine', 'table', 'rows', 'million', 'data length', 'MB', 'index']
        return {'header': header, 'tables': self.fetchall('call mem')}

    def last_loaded(self, dbname='DATABASE()'):
        return self.fetchID("select event_time as ID from "+dbname+"." + "log where entity_name = 'load_database.sh' and message = 'done' order by idx desc limit 1")

    def PMID(self, sql):
        '''
        For given sql select query, return a list of unique PMID strings.
        '''
        pubmeds = set()
        for row in self.fetchall(sql):
            pubmeds.add(str(row['PMID']))
        return pubmeds

    def hgvs_text(self, sql):
        """
        For given sql select query, return a list of unique hgvs_text strings.
        """
        hgvs_texts = set()
        for row in self.fetchall(sql):
            hgvs_texts.add(str(row['hgvs_text']))
        return hgvs_texts

    def trunc_str(self, inp, maxlen):
        '''
        Useful utility method for storing text in a database
        :param inp: a string
        :param maxlen: the max length of that string
        :return: '"%s"' % s or '"%s..."' % s[:m-3]
        '''
        if maxlen < 3:
            raise RuntimeError('maxlen must be at least 3')
        if len(inp) > maxlen:
            inp = '%s...' % inp[:m-3]
        return inp

    def str_or_null(self, inp, truncate_str=False, maxlen=200):
        '''
        Useful utility method for MySQL insertion statements text in a database

        :param None or some object with __str__ implemented
        :param truncate_str: whether to truncate string to maxlen
        :param maxlen: the max length for a string [default: 200]
        :return: 'null' or '"%s"' % t or '"%s..."' % t[:m-3]
        '''
        if inp is None:
            return 'null'
        #inp = mdb.escape_string(str(inp))
        inp = EscapeString(inp)
        if truncate_str:
            inp = self.trunc_str(str(inp), maxlen)
        return '"%s"' % str(inp)

    def get_last_mirror_time(self, entity_name):
        '''
        Get the last time some entity was mirrored

        :param entity_name: table name for db, for example, bic_brca1
        :return: datetime if found
        '''
        sql_query = 'SELECT event_time FROM log WHERE entity_name = "%s" AND message like "rows loaded %" ORDER BY event_time DESC limit 1' % entity_name
        result = self.fetchrow(sql_query)
        if result:
            return result['event_time']
        raise RuntimeError('Query "%s" returned no results. Have you loaded the %s table?' % (sql_query, entity_name))

