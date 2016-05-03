import psycopg2
from psycopg2 import extras
import os

def query(conn, sql, results='nt', print_sql=False):
    "Issue SQL query that returns a result set."
    return execute(conn, sql, print_sql=print_sql, results=results)

def execute(conn, sql, data=None, print_sql=False, submit=True, results=False):
    "Issue a general SQL statment. Optinally specify results cursor type."
    with conn:
        if results:
            if results == 'pgdict':
                cur_type = psycopg2.extras.DictCursor
            elif results == 'dict':
                cur_type = psycopg2.extras.RealDictCursor
            elif results == 'logging':
                cur_type = psycopg2.extras.LoggingCursor
            else:
                cur_type = psycopg2.extras.NamedTupleCursor
            with conn.cursor(cursor_factory=cur_type) as cursor:
                if print_sql:
                    return curr.mogrify(query=sql, vars=data)
                if submit:
                    cursor.execute(query=sql, vars=data)
                    return cursor.fetchall()
        else:
            with conn.cursor() as cursor:
                if print_sql:
                    print(cursor.mogrify(query=sql, vars=data))
                if submit:
                    cursor.execute(query=sql, vars=data)

def exec_psql(conn, sql_path, print_sql=False, submit=True, **kwargs):
    "Execute a parameterized .psql file"
    print('PG Executeing: '+sql_path)
    with open(sql_path, 'r') as sql_file:
        sql_template = sql_file.read()
    sql = sql_template.format(**kwargs)
    execute(conn, sql, print_sql=print_sql, submit=submit)

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

def drop_table(conn, table, schema='public', print_sql=True):
    "Issue 'drop table if exists' statment."
    sql = "drop table if exists {schema}.{table};".format(schema=schema, table=table)
    execute(conn, sql, print_sql=print_sql)

def drop_schema(conn, schema, print_sql=True):
    "Issue 'drop schema if exists .. cascade' statment."
    sql = "drop schema if exists {schema} cascade;".format(schema=schema)
    execute(conn, sql, print_sql=print_sql)

def vacuum(conn, table, schema='public'):
    "Vacume & analyze table"
    execute(conn, sql="vacuum analyze {schema}.{table};".format(schema=schema, table=table))
