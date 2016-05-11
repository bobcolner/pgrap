import psycopg2
import os
_logger = logging.getLogger(__name__)

def query(conn, sql, results='nt', print_sql=False):
    "Issue SQL query that returns a result set."
    return execute(conn, sql, results=results)

def execute(conn, sql, data=None, results=False):
    "Issue a general SQL statment. Optinally specify results cursor type."
    with conn:
        if results:
            from psycopg2 import extras
            if results == 'pgdict':
                cur_type = psycopg2.extras.DictCursor
            elif results == 'dict':
                cur_type = psycopg2.extras.RealDictCursor
            elif results == 'logging':
                cur_type = psycopg2.extras.LoggingCursor
            else:
                cur_type = psycopg2.extras.NamedTupleCursor
            with conn.cursor(cursor_factory=cur_type) as cursor:
                cursor.execute(query=sql, vars=data)
                _logger.info('fetching results: {0}'.format(sql))
                return cursor.fetchall()
        else:
            with conn.cursor() as cursor:
                cursor.execute(query=sql, vars=data)
                _logger.info('executing statment: {0}'.format(sql))

def exec_psql(conn, sql_path, print_sql=False, submit=True, **kwargs):
    "Execute a parameterized .psql file"
    with open(sql_path, 'r') as sql_file:
        sql_template = sql_file.read()
    sql = sql_template.format(**kwargs)
    _logger.info('executing psql file: {0}'.format(sql_path))
    execute(conn, sql, submit=submit)

def multi_insert(conn, data, table, column_list, schema='public', submit=True):
    "Issue a multi-row insert"
    # http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
    values = ",".join(["%s"] * len(data[0]))
    sql = '''insert into {schema}.{table} ({columns}) values ({values})
    '''.format(table=table, schema=schema, columns=column_list, values=values)
    execute(conn, sql, data=data, submit=submit)

def copy_from(conn, file_obj, table, columns, sep="\t"):
    "Stream file_obj into table"
    with conn:
        with conn.cursor() as cursor:
            cursor.copy_from(file=file_obj, table=table, columns=columns, sep=sep)
            _logger.info('psql copy to table: {0}'.format(table))

def drop_table(conn, table, schema='public'):
    "Issue 'drop table if exists' statment."
    sql = "drop table if exists {schema}.{table};".format(schema=schema, table=table)
    execute(conn, sql)
    _logger.info('dropped table: {0}'.format(table))

def drop_schema(conn, schema):
    "Issue 'drop schema if exists .. cascade' statment."
    sql = "drop schema if exists {schema} cascade;".format(schema=schema)
    execute(conn, sql)
    _logger.info('dropped schema: {0}'.format(schema))

def vacuum(conn, table, schema='public'):
    "Vacume & analyze table"
    execute(conn, sql="vacuum analyze {schema}.{table};".format(schema=schema, table=table))
