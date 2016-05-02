import io
import csv
import jsonpickle
import tqdm
from pgrap import pgrap

def create_doc(conn, table, schema='public'):
    sql = '''
create schema if not exists {schema};
create table if not exists {schema}.{table} (
    id serial primary key
    , doc jsonb
    , created_at timestamp(0) without time zone default (clock_timestamp() at time zone 'utc')
    , updated_at timestamp(0) default null
    , updated_count int default 0
);
create index if not exists doc_gin_idx on {schema}.{table} using gin(doc jsonb_path_ops);

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
'''.format(schema=schema, table=table)
    
    pgrap.execute(conn, sql, print_sql=False)

def insert_doc(conn, data, table, schema='public'):
    sql = '''
insert into {schema}.{table} (doc) values (%s);'''.format(schema=schema, table=table)
    pgrap.execute(conn, sql, data=(data,), print_sql=False)

def insert_multi_doc(conn, data, table='copy_temp', schema='public', overwrite=False):
    if overwrite:
        pgrap.drop_table(conn, table, schema)
        create_doc(conn, table, schema)
    
    for row in tqdm.tqdm(data, desc='Inserting EightyCraw Stagging Data'):
        try:
            insert_doc(conn, data=row, table=table, schema=schema)
        except:
            print("Fail")
            continue

def write_tsv(fio, data):
    writer = csv.writer(fio, dialect='excel')
    for row in tqdm.tqdm(data, desc='Writing TSV'):
        writer.writerow(jsonpickle.dumps(row, False))
    return fio

def data2tsv(data, output='buffer'):
    if output=='buffer':
        with io.StringIO() as fio:
            return write_tsv(fio, data)
    elif output=='file':
        with open("copy_tmp.tsv", "w") as fio:
            return write_tsv(fio, data)
