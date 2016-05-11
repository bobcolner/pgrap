# pgrap
High-level functions to access Postgres Capabilities.

#### features
- Simple to use funcation acess to postgres db
- Easy & preformant key-value API. Requires PG9.5 to for indexed JSONB type and native upserts.

# S3rap
AWS S3 convenience functions based on boto3.

#### install
```sh
pip install pgrap
```

#### examples
```py
from pgrap import pgrap, pgkv

# connect via psycopg2
conn = pgrap.psycopg2.connect(...)
```

###### PG KV functions
```py
# insert data into key-value table. Optionally create / overwrite table.
pgkv.insert_kv(conn, k_data, v_data, table='kv', schema='public', dtype='auto', setup='create')

# find record with key
pgkv.find_kv(conn, table, key, select='*', schema='public')

# full-text search (request text value type)
pgkv.fulltext_search_kv(conn, search, table, schema='public', select='*', limit=False)

# insert multiple key-value records (loop)
pgkv.insert_multi_kv(conn, data, k_name, table='kv', schema='public', dtype='jsonb', setup='create')

# create new key-value table
pgkv.create_kv(conn, table='kv', schema='public', dtype='jsonb')
```

