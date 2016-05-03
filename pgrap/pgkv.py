import io
import csv
import jsonpickle
import tqdm
from . import pgcore

MAX_KEY = 2048

def create_kv(conn, table='kv', schema='public', dtype='jsonb'):    
    sql = '''
create schema if not exists {schema};
create table if not exists {schema}.{table} (
    id serial primary key
    , key varchar({n}) not null unique
    , value {dtype}
    , created_at timestamp(0) without time zone default (clock_timestamp() at time zone 'utc')
    , updated_at timestamp(0) default null
    , updated_count int default 0
);
create index if not exists idx_btree_key on {schema}.{table} using btree(key);

create or replace function set_updated_at() returns trigger as $$
begin
    new.updated_at := clock_timestamp();
    return new;
end;
$$ language plpgsql;
drop trigger if exists set_updated_at on {schema}.{table};
create trigger set_updated_at before update on {schema}.{table} for each row execute procedure set_updated_at();

create or replace function inc_updated_count() returns trigger as $$
begin
    new.updated_count := new.updated_count + 1;
    return new;
end;
$$ language plpgsql;
drop trigger if exists inc_updated_count on {schema}.{table};
create trigger inc_updated_count before update on {schema}.{table} for each row execute procedure inc_updated_count();

'''.format(schema=schema, table=table, dtype=dtype, n=MAX_KEY)

    # add optional indexes
    if dtype == 'text':
        sql = sql + '''
create index if not exists idx_tsvector_value on {schema}.{table} using gin(to_tsvector('english', value));
create index if not exists idx_trgm_value on {schema}.{table} using gin(value gin_trgm_ops);'''.format(schema=schema, table=table)
    elif dtype == 'jsonb':
        sql = sql + '''
create index if not exists idx_gin_value on {schema}.{table} using gin(value jsonb_path_ops);'''.format(schema=schema, table=table)
    
    pgcore.execute(conn, sql)

def kv_setup(conn, table='kv', schema='public', dtype='jsonb', setup='create'):
    if setup == 'create':
        create_kv(conn, table, schema, dtype)
    elif setup == 'overwrite':
        pgcore.drop_table(conn, table, schema)
        create_kv(conn, table, schema, dtype)
    elif setup == 'drop':
        pgcore.drop_table(conn, table, schema)

def insert_kv(conn, k_data, v_data, table='kv', schema='public', dtype='auto', setup='create'):
    
    if len(str(k_data)) > MAX_KEY:
        print('key data to large: '+str(k_data)[0:MAX_KEY])
        return None
    
    if dtype == 'auto':    
        if type(v_data) == str:
            dtype = 'text'
        else:
            dtype = 'jsonb'
    
    if dtype == 'jsonb':
        v_data = jsonpickle.encode(v_data, False)
    
    if setup:
        kv_setup(conn, table, schema, dtype, setup)

    sql = '''
insert into {schema}.{table} (key, value) values (%s, %s)
on conflict (key) do update set 
    value = excluded.value
;'''.format(schema=schema, table=table)
    
    pgcore.execute(conn, sql, data=(str(k_data), v_data))

def insert_multi_kv(conn, data, k_name, table='kv', schema='public', dtype='jsonb', setup='create'):
    if setup:
        kv_setup(conn, table, schema, dtype, setup)

    for row in tqdm.tqdm(data, desc='Inserting Key-Value Records'):
        try:
            insert_kv(conn, k_data=row[k_name], v_data=row, table=table, schema=schema, dtype=dtype, setup=None)
        except:
            print("Fail: "+row[k_name])
            continue

def find_kv(conn, table, key, select='*', schema='public'):
    sql = '''select {select} from {schema}.{table} where key = '{key}';
    '''.format(select=select, key=key, table=table, schema=schema)
    return pgcore.query(conn, sql)

def fulltext_search_kv(conn, search, table, schema='public', select='*', limit=False):
    search = search.replace(' ', '&')
    sql = '''select {select} from {schema}.{table} where to_tsvector(value) @@ to_tsquery('{search}');
    '''.format(schema=schema, table=table, select=select, search=search)
    if limit:
        sql = sql + "\nlimit {limit}".format(limit=limit)
    return pgcore.query(conn, sql)
