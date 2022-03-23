import pandas as pd
from ConfigParser import ConfigParser
import os
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"  # to be placed before import cx_Oracle, necessary to use queries with unicode.
# see https://github.com/oracle/python-cx_Oracle/issues/36 and https://stackoverflow.com/questions/49174710/how-to-handle-unicode-data-in-cx-oracle-and-python-2-7
import cx_Oracle

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

    if connection_type=='oracle':
        # Basic connection, works for read only queries.
        connection_str = "{user}/{password}@{host}:{port}/{service}".format(**params)
        return cx_Oracle.connect(connection_str, encoding = "UTF-8", nencoding = "UTF-8")
    elif connection_type=='sqlalchemy':
        # Required to create table in oracle using df.to_sql()
        from sqlalchemy import create_engine
        connection_str = "oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}".format(**params)  # Ex: 'oracle+cx_oracle://scott:tiger@host:1521/?service_name=hr'
        conn = create_engine(connection_str, encoding='UTF8')
        return conn
    else:
        raise ValueError('connection_type is not accepted: %s'%connection_type )

def query(query_str, **connect_args):
    """
    Run an sql query in oracle and return output to pandas table (dataframe).
    """
    connection = connect(**connect_args)

    if connection.__class__.__doc__ is not None and connection.__class__.__doc__.__contains__('sqlalchemy'):  # hasattr(connection, '__class__')
        df = pd.read_sql(query_str, connection, coerce_float=True)
        return df
    else :
        # TODO: fix rare pb "ValueError: invalid literal for int() with base 10: 'xxx.xx'"
        cursor = connection.cursor()
        try:
            cursor.execute(query_str)
        except cx_Oracle.DatabaseError as e:
            raise cx_Oracle.DatabaseError, 'Failed running query, received error "%s"'%e

        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
        connection.close()
        return df

if __name__ == "__main__":
    df = query('SELECT * FROM all_tables', db='name_of_connection_from_credentials_file')
    print(df)
