import pandas as pd
from configparser import ConfigParser
import os
from sqlalchemy import create_engine

def connect(db, creds_or_file='conf/connections.cfg'):
    config = ConfigParser()
    if isinstance(creds_or_file, str):
        assert os.path.isfile(creds_or_file)
        config.read(creds_or_file)
    else:
        config = creds_or_file

    params = {
        'user':     config.get(db, 'user'),
        'password': config.get(db, 'password'),
        'host':     config.get(db, 'host'),
        'port':     config.get(db, 'port'),
        'service':  config.get(db, 'service')
    }

    connection_str = "redshift+psycopg2://{user}:{password}@{host}:{port}/{service}".format(**params)
    conn = create_engine(connection_str, encoding='UTF8')
    return conn

def query(query_str, **connect_args):
    connection = connect(**connect_args)
    df = pd.read_sql(query_str, connection)
    return df

if __name__ == "__main__":
    df = query('SELECT * FROM yourtable', db='name_of_connection_from_credentials_file')
    print(df)
