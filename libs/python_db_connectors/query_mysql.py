import pandas as pd
from configparser import ConfigParser
import os

def connect(db, connection_type='sqlalchemy'):
    config = ConfigParser()
    config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'credentials.cfg'))

    params = {
        'user':     config.get(db, 'user'),
        'password': config.get(db, 'password'),
        'host':     config.get(db, 'host'),
        'port':     config.get(db, 'port'),
        'service':  config.get(db, 'service')
    }

    # sqlEngine       = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
    # dbConnection    = sqlEngine.connect()

    from sqlalchemy import create_engine
    import pymysql


    # print('asdfasdf1')
    # # db_connection_str = 'mysql+pymysql://mysql_user:mysql_password@mysql_host/mysql_db'
    # db_connection_str = "mysql+pymysql://{user}:{password}@{host}/{service}".format(**params)
    # print('asdfasdf1b', db_connection_str)
    # db_connection = create_engine(db_connection_str)
    # df = pd.read_sql('SELECT id FROM users', con=db_connection)
    # print('asdfasdf2')
    # import ipdb; ipdb.set_trace()

    # connection_str = "oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}".format(**params)  # Ex: 'oracle+cx_oracle://scott:tiger@host:1521/?service_name=hr'
    connection_str = "mysql+pymysql://{user}:{password}@{host}/{service}".format(**params)  # Ex: 'oracle+cx_oracle://scott:tiger@host:1521/?service_name=hr'
    conn = create_engine(connection_str, encoding='UTF8')
    return conn

def query(query_str, **connect_args):
    connection = connect(**connect_args)
    df = pd.read_sql(query_str, connection);
    # import ipdb; ipdb.set_trace()
    # connection.close()
    return df

if __name__ == "__main__":
    # df = query('SELECT * FROM all_tables', db='name_of_connection_from_credentials.cfg')
    df = query('SELECT id FROM users', db='mysql_aprevot')
    print(df)
