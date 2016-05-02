import psycopg2
import urllib

# postgres connection ultilities
def docker_connect(autocommit=False):
    "Connect to database running in docker container named 'pg'"
    conn = psycopg2.connect(user=os.environ['PG_ENV_POSTGRES_USER'], 
        password=os.environ['PG_ENV_POSTGRES_PASSWORD'],
        host=os.environ['PG_PORT_5432_TCP_ADDR'], 
        port=os.environ['PG_PORT_5432_TCP_PORT'], 
        database=os.environ['PG_ENV_POSTGRES_DB'])
    conn.autocommit = autocommit
    return conn
    
def conn_config(username, password, hostname, port='5432', dbname='postgres', scheme='postgres'):
    conn_config = {
        'username': username, 
        'password': password, 
        'hostname': hostname,
        'port': port, 
        'dbname': dbname, 
        'scheme': scheme }
    return conn_config

def local_config():
    return conn_config(username='postgres', password='postgres', hostname='localhost', 
        port=5432, dbname='postgres', scheme='postgres')

def docker_config():
    return conn_config(
        username=os.environ['PG_ENV_POSTGRES_USER'], 
        password=os.environ['PG_ENV_POSTGRES_PASSWORD'],
        hostname=os.environ['PG_PORT_5432_TCP_ADDR'], 
        port=os.environ['PG_PORT_5432_TCP_PORT'], 
        dbname=os.environ['PG_ENV_POSTGRES_DB'], 
        scheme='postgres')

def str_to_config(conn_str):
    conn_parse = urllib.parse.urlparse(url=conn_str)
    conn_config = {
        'username': conn_parse.username, 
        'password': conn_parse.password, 
        'hostname': conn_parse.hostname,
        'port': conn_parse.port, 
        'dbname': conn_parse.path.strip('/'), 
        'scheme': conn_parse.scheme }
    return conn_config

def config_to_str(conn_config):
    config_str = "{scheme}://{username}:{password}@{hostname}:{port}/{dbname}".format(
        username = conn_config['username'], 
        password = conn_config['password'], 
        hostname = conn_config['hostname'],
        port = conn_config['port'], 
        dbname = conn_config['dbname'], 
        scheme = conn_config['scheme'])
    return config_str

def connect(conn_config, autocommit=False):
    conn = psycopg2.connect(
        database = conn_config['dbname'], 
        user = conn_config['username'], 
        password = conn_config['password'], 
        host = conn_config['hostname'])
    conn.autocommit = autocommit
    return conn

# Alt. DB APIs
def dataset_connect(conn_config):
    import dataset
    config_str = config_to_str(conn_config)
    return dataset.connect(config_str)

def postpy_connect(conn_config):
    import postgres as postgres_py
    config_str = config_to_str(conn_config)
    return postgres_py.Postgres(config_str)

def dbpy_connect(conn_config, system_tables=False):
    import db as dbpy
    conn = dbpy.DB(username = conn_config['username'], 
        password = conn_config['password'], hostname = conn_config['hostname'], 
        scheme = conn_config['scheme'], dbname = conn_config['dbname'],
        exclude_system_tables = not system_tables)
    return conn

def queries_connect(conn_config):
    import queries
    config_str = config_to_str(conn_config)
    return queries.Session(config_str)

def sqlalchemy_connect(conn_config, output='conn'):
    import sqlalchemy
    config_str = config_to_str(conn_config)
    engine = sqlalchemy.create_engine(config_str)
    if output == 'conn':
        return engine.connect()
    else:
        return engine

# Other db libs
# https://github.com/kennethreitz/records
# https://github.com/paulchakravarti/pgwrap
