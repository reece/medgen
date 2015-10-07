# -*- coding: utf-8 -*-
# from __future__ import absolute_import
from __future__ import unicode_literals, print_function

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
    value = value.encode("utf-8")
    value = mdb.escape_string(value)
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
        return results[0] if len(results) > 0 else None

    def fetchID(self, select_sql, id_colname='ID'):
        results = self.fetchrow(select_sql)
        if results is not None:
            if id_colname in results:
                return results[id_colname]
            else:
                raise RuntimeError("No ID column found.  SQL query: %s" % select_sql)
        return None  # no results found


    def list_concepts(self, select_sql):
        """
        Fetch list of concepts
        :param select_sql: query
        :return: list cui
        """
        return self.fetchlist(select_sql, 'CUI')

    def list_genes(self, select_sql):
        """
        Fetch list of genes
        :param select_sql: query
        :return: list HGNC
        """
        return self.fetchlist(select_sql, 'gene_name')

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
            fields.append(k)
            values.append(v)

        sql = 'insert into {} ({}) values ({});'.format(tablename, ','.join(fields), ','.join(['%s' for v in values]))
        queryobj = self.execute(sql, values)
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

        for k, v in field_value_dict.items():
            clause = '%s=' % k
            # surround strings and datetimes with quotation marks
            if v == None:
                clause += 'NULL'
            elif hasattr(v, 'strftime'):
                clause += '"%s"' % v.strftime(SQLDATE_FMT)
            elif hasattr(v, 'lower'):
                clause += EscapeString(v)  #surrounds strings with quotes and unicodes them.
            else:
                clause += unicode(v)
            clauses.append(clause)

        sql = 'update %s set %s where %s=%i;' % (tablename, ', '.join(clauses), id_col_name, row_id)
        queryobj = self.execute(sql)
        # retrieve and return the row id of the insert. returns 0 if insert failed.
        return queryobj.lastInsertID

    def delete(self, tablename, field_value_dict):
        '''
        :param: tablename: name of table to receive new row
        :param: field_value_dict: map of field=value
        :return: row_id (integer) (returns 0 if insert failed)
        '''
        if len(field_value_dict) == 0:
            raise RuntimeError("Do not support delete without a WHERE clause")

        where_sql = ''
        for k, v in field_value_dict.items():
            if v == None:
                v = 'NULL'
                where_sql += 'AND {} is NULL '.format(k)
            # surround strings and datetimes with quotation marks
            elif hasattr(v, 'strftime'):
                v = '"%s"' % v.strftime(SQLDATE_FMT)
                where_sql += 'AND {}={} '.format(k, v)
            elif hasattr(v, 'lower'):
                v = EscapeString(v)  # surrounds strings with quotes and unicodes them.
                where_sql += 'AND {}={} '.format(k, v)
            else:
                v = unicode(v)
                where_sql += 'AND {}={} '.format(k, v)

        where_sql = where_sql[len('AND '):]

        sql = 'delete from {} where {};'.format(tablename, where_sql)

        log.debug(sql)
        queryobj = self.execute(sql)
        # retrieve and return the row id of the insert. returns 0 if insert failed.
        return queryobj.lastInsertID

    def drop_table(self, tablename):
        return self.execute(" drop table if exists " + tablename)

    def truncate(self, tablename):
        return self.execute(" truncate " + tablename)

    def execute(self, sql, args=None):
        '''
        Excutes arbitrary sql string in current database connection.    
        Returns results as PySQLPool query object.
        '''
        log.debug('SQL.execute ' + sql)
        queryobj = PySQLPool.getNewQuery(self.connect())
        queryobj.Query(sql, args)
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
        return self.fetchID(
            "select event_time as ID from " + dbname + "." + "log where entity_name = 'load_database.sh' and message = 'done' order by idx desc limit 1")

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
            inp = '%s...' % inp[:maxlen - 3]
        return inp

    def get_last_mirror_time(self, entity_name):
        '''
        Get the last time some entity was mirrored

        :param entity_name: table name for db, for example, bic_brca1
        :return: datetime if found
        '''
        sql_query = 'SELECT event_time FROM log WHERE entity_name = "?" AND message like "rows loaded %" ORDER BY event_time DESC limit 1'.replace(
            '?', entity_name)
        result = self.fetchrow(sql_query)
        if result:
            return result['event_time']
        raise RuntimeError('Query "%s" returned no results. Have you loaded the %s table?' % (sql_query, entity_name))


    def create_index(self, table, colspec):
        """
        Create index on a specified table using the colums defined.
        Index start/stop times are logged to the "log" table.
        :param table: name of the table, example, "train"
        :param colspec: name of column, for example, "RQ"
        :return:
        """
        self.execute("call create_index('%s', '%s') " % (table, colspec))

    def fetchlist(self, select_sql, column='gene_name'):
        """
        Fetch as list
        :param select_sql: query
        :param column: name of column you want to make a list out of
        :return: list
        """
        rows = self.fetchall(select_sql)
        return [] if rows is None else [str(r[column]) for r in rows]