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
conn = pgrap.psycopg2.connect(...)
```

###### PG KV functions
```py
# insert data into key-value table. Optionally create / overwrite table.
pgkv.insert_kv(conn, k_data, v_data, table='kv', schema='public', dtype='auto', setup='create')

# create new key-value table
pgkv.create_kv(conn, table='kv', schema='public', dtype='jsonb')
# 
```

