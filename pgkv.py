import jsonpickle
import tqdm
from pgrap import pgrap

def create_kv(conn, table, schema='public', dtype='jsonb', index=True, load_type='copy', print_sql=True):
    if load_type == 'copy':
        unique = ''
    else:
        unique = 'unique'
    
    sql_base = '''
create schema if not exists {schema};
create table if not exists {schema}.{table} (
    id serial primary key
    , key varchar(2048) not null {unique}
    , value {dtype}'''.format(schema=schema, table=table, dtype=dtype, unique=unique)
    
    sql_copy = "\n);"
    
    sql_insert = '''    
    , created_at timestamp(0) without time zone default (clock_timestamp() at time zone 'utc')
    , updated_at timestamp(0) default null
    , updated_count int default 0
);
create or replace function set_updated_at() returns trigger as $$
begin
    new.updated_at := clock_timestamp();
    return new;
end;
$$ language plpgsql;
drop trigger if exists set_updated_at on {schema}.{table};
create trigger set_updated_at before update on {schema}.{table} for each row execute procedure set_updated_at();
'''.format(schema=schema, table=table, dtype=dtype)
        
    if load_type == 'copy':
        sql = sql_base + sql_copy
    elif load_type != 'copy':
        sql = sql_base + sql_insert
    # add optional indexes
    if index:
        sql = sql + '''
create index if not exists idx_{table}_key on {schema}.{table} using btree(key);'''.format(schema=schema, table=table)
    if index and dtype == 'text':
        sql = sql + '''
create index if not exists idx_{table}_value_tsv on {schema}.{table} using gin(to_tsvector('english', value));
create index if not exists idx_{table}_value_trgm on {schema}.{table} using gin(value gin_trgm_ops);'''.format(schema=schema, table=table)
    elif index and dtype == 'jsonb':
        sql = sql + '''
create index if not exists idx_{table}_value_json on {schema}.{table} using gin(value jsonb_path_ops);'''.format(schema=schema, table=table, dtype=dtype)
    # if print_sql:
    #     print(sql)
    pgrap.execute(conn, sql, print_sql=print_sql)

def insert_kv(conn, k_data, v_data, table, schema='public', dtype='auto', print_sql=False, upsert=True, index=True):
    if len(str(k_data)) > 2048:
        print('key data to large: '+str(k_data)[0:2048])
        return None
    if dtype == 'auto':    
        if type(v_data) == str:
            dtype = 'text'
        else:
            dtype = 'jsonb'
            v_data = jsonpickle.encode(v_data, False)

    sql = "insert into {schema}.{table} (key, value) values (%s, %s)".format(schema=schema, table=table)
    if upsert:
        sql = sql + '''on conflict (key) do update set 
    value = excluded.value, 
    updated_count = {schema}.{table}.updated_count + 1;'''.format(schema=schema, table=table)
    key_value = (str(k_data), v_data)
    pgrap.execute(conn, sql, data=key_value, print_sql=print_sql)
    
def insert_multi_kv(conn, data, k_name, table='copy_temp', schema='public', overwrite=False):
    if overwrite:
        pgrap.drop_table(conn, table, schema)
        pgrap.create_kv(conn, table, schema, dtype='jsonb', index=False, load_type='insert')
    
    for page in tqdm.tqdm(data, desc='Inserting EightyCraw Stagging Data'):
        pgrap.insert_kv(conn, k_data=page[k_name], v_data=page, table=table, schema=schema)

def copy_multi_kv(conn, data, k_name, table='copy_temp', overwrite=False):
    if overwrite:
        pgrap.drop_table(conn, table, schema='public')
        pgrap.create_kv(conn, table, schema='public', dtype='jsonb', index=False, load_type='copy')
    
    def gen_records(records):
        for value in records:
            if len(str(value[k_name])) > 2048:
                print('key data to large: '+str(k_data)[0:2048])
                return None
            yield '{}\t{}'.format(value[k_name], jsonpickle.dumps(value).replace('"', r'\"'))
    
    fio = io.StringIO('\n'.join(gen_records(data)))
    pgrap.copy_from(conn, file_obj=fio, table=table, columns=('key', 'value'))
    fio.close()

def search_kv(conn, search, table, select='*', limit=False, schema='public'):
    search = search.replace(' ', '&')
    sql = '''select {select} from {schema}.{table} where to_tsvector(value) @@ to_tsquery('{search}');
    '''.format(schema=schema, table=table, select=select, search=search)
    if limit:
        sql = sql + "\nlimit {limit}".format(limit=limit)
    return pgrap.query(conn, sql)

def find_kv(conn, table, key, select='*', schema='public'):
    sql = '''select {select} from {schema}.{table} where key = {key};
    '''.format(select=select, key=key, table=table, schema=schema)
    return pgrap.query(conn, sql)

def select_kv(conn, table, select='*', where='true', orderby=False, limit=False, schema='public'):
    sql = '''select {select} from {schema}.{table} where {where};
    '''.format(schema=schema, table=table, select=select, where=where)
    if orderby:
        sql = sql + "\norder by {orderby}".format(orderby=orderby)
    if limit:
        sql = sql + "\nlimit {limit}".format(limit=limit)
    return pgrap.query(conn, sql)
