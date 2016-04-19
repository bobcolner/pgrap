import psycopg2
from psycopg2 import extras
import os
import io

def docker_connect(autocommit=False):
    conn = psycopg2.connect(user=os.environ['PG_ENV_POSTGRES_USER'], 
        password=os.environ['PG_ENV_POSTGRES_PASSWORD'],
        host=os.environ['PG_PORT_5432_TCP_ADDR'], 
        port=os.environ['PG_PORT_5432_TCP_PORT'], 
        database=os.environ['PG_ENV_POSTGRES_DB'])
    conn.autocommit = autocommit
    return conn

def query(conn, sql, results='nt', print_sql=False):
    return execute(conn, sql, print_sql=print_sql, results=results)

def execute(conn, sql, data=None, print_sql=False, submit=True, results=False):
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
                    print(curr.mogrify(query=sql, vars=data))
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
    print('PG Executeing: '+sql_path)
    with open(sql_path, 'r') as sql_file:
        sql_template = sql_file.read()
    sql = sql_template.format(**kwargs)
    execute(conn, sql, print_sql=print_sql, submit=submit)

def multi_insert(conn, data, table, column_list, schema='public', submit=True):
    # http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
    values = ",".join(["%s"] * len(data[0]))
    sql = '''
    insert into {schema}.{table} ({columns}) values ({values})
    '''.format(table=table, schema=schema, columns=column_list, values=values)
    execute(conn, sql, data=data, submit=submit)

def copy_from(conn, file_obj, table, columns=None, schema='public', header='OFF', sep="\t"):
    with conn:
        with conn.cursor() as cursor:
            # copy_sql = '''
            # copy {schema}.{table} {columns} from stdin with
            #     format csv
            #     delimiter '{sep}'
            #     header {header}
            #     null ''
            # '''.format(schema=schema, table=table, columns=columns, header=header, sep=sep)
            # print(copy_sql)
            # cursor.copy_expert(sql=copy_sql, file=file_obj)
            cursor.copy_from(file=file_obj, table=table, columns=columns, sep=sep)

def drop_table(conn, table, schema='public'):
    sql = "drop table if exists {schema}.{table};".format(schema=schema, table=table)
    execute(conn, sql)        

def vacuum(conn, table, schema='public'):
    with conn:
        execute(conn, sql="vacuum analyze {schema}.{table};".format(schema=schema, table=table))
